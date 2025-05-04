# Sound Monitoring System - Data Processing Center
# Implements the center for processing data from sound monitoring sensors

import json
import sqlite3
import pika
import time
import math
import logging
from datetime import datetime
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('sound_monitoring_center')


# Database setup
class DatabaseManager:
    def __init__(self, config_path='config.ini'):
        self.config = self.load_config(config_path)
        self.db_path = self.config['path']
        self.init_db()

    def load_config(self, config_path):
        """Load database configuration from INI file"""
        import configparser
        config = configparser.ConfigParser()
        config.read(config_path)

        # Get database configuration
        if 'DATABASE' in config:
            db_config = {
                'path': config['DATABASE'].get('path', 'monitoring.db')
            }
        else:
            # Default configuration
            db_config = {
                'path': 'monitoring.db'
            }

        logger.info(f"Loaded database configuration: {db_config}")
        return db_config

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def init_db(self):
        """Initialize the database with required tables"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Table for sensor devices
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sensors (
            id TEXT PRIMARY KEY,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            last_active TIMESTAMP
        )
        ''')

        # Check if detections table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='detections'")
        detections_exists = cursor.fetchone() is not None

        # Table for sound detections
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS detections (
            id TEXT PRIMARY KEY,
            sensor_id TEXT,
            timestamp INTEGER,
            latitude REAL,
            longitude REAL,
            altitude REAL,
            sound_type TEXT,
            confidence REAL,
            key_frequencies TEXT,
            amplitude REAL,
            processed BOOLEAN DEFAULT 0,
            is_retry BOOLEAN DEFAULT 0,
            original_timestamp INTEGER,
            FOREIGN KEY (sensor_id) REFERENCES sensors (id)
        )
        ''')

        # If detections table already existed, check if it needs migration
        if detections_exists:
            # Check if key_frequencies column exists
            try:
                cursor.execute("SELECT key_frequencies FROM detections LIMIT 1")
            except sqlite3.OperationalError:
                # Column doesn't exist, add it
                cursor.execute("ALTER TABLE detections ADD COLUMN key_frequencies TEXT")
                logger.info("Added key_frequencies column to detections table")

        # Table for movement analysis
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_detection_id TEXT,
            speed REAL,
            direction REAL,
            prediction_latitude REAL,
            prediction_longitude REAL,
            prediction_timestamp INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (source_detection_id) REFERENCES detections (id)
        )
        ''')

        # Table for acknowledgments
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS acknowledgments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            packet_id TEXT UNIQUE,
            sensor_id TEXT,
            timestamp INTEGER,
            FOREIGN KEY (sensor_id) REFERENCES sensors (id)
        )
        ''')

        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")

    def register_sensor(self, sensor_id, latitude, longitude, altitude):
        """Register a new sensor or update existing one"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
        INSERT OR REPLACE INTO sensors 
        (id, latitude, longitude, altitude, last_active) 
        VALUES (?, ?, ?, ?, ?)
        ''', (sensor_id, latitude, longitude, altitude, datetime.now()))

        conn.commit()
        conn.close()
        logger.info(f"Sensor {sensor_id} registered at {latitude}, {longitude}")

    def store_detection(self, detection_data):
        """Store a new sound detection"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Extract data from the detection JSON
        detection_id = detection_data["packet_id"]
        sensor_id = detection_data["device_id"]
        timestamp = detection_data["timestamp"]
        latitude = detection_data["location"]["latitude"]
        longitude = detection_data["location"]["longitude"]
        altitude = detection_data["location"]["altitude"]
        sound_type = detection_data["detection"]["type"]
        confidence = detection_data["detection"]["confidence"]
        key_frequencies = json.dumps(detection_data["detection"]["spectrum_key_frequencies"])
        amplitude = detection_data["detection"]["amplitude"]

        # Check if this is a retry
        is_retry = detection_data.get("retry", False)
        original_timestamp = detection_data.get("original_timestamp", timestamp)

        # Update sensor location
        self.register_sensor(sensor_id, latitude, longitude, altitude)

        # Store detection
        cursor.execute('''
        INSERT OR REPLACE INTO detections
        (id, sensor_id, timestamp, latitude, longitude, altitude, 
         sound_type, confidence, key_frequencies, amplitude, is_retry, original_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (detection_id, sensor_id, timestamp, latitude, longitude,
              altitude, sound_type, confidence, key_frequencies, amplitude,
              is_retry, original_timestamp))

        conn.commit()
        conn.close()
        logger.info(f"Detection {detection_id} stored from sensor {sensor_id}")
        return detection_id

    def record_acknowledgment(self, packet_id, sensor_id):
        """Record that a packet was acknowledged"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
        INSERT OR REPLACE INTO acknowledgments
        (packet_id, sensor_id, timestamp)
        VALUES (?, ?, ?)
        ''', (packet_id, sensor_id, int(time.time())))

        conn.commit()
        conn.close()
        logger.info(f"Acknowledgment recorded for packet {packet_id}")

    def get_recent_detections(self, sound_type, limit=5):
        """Get recent detections of a specific type, ordered by timestamp"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
        SELECT id, sensor_id, timestamp, latitude, longitude, altitude, confidence, amplitude
        FROM detections
        WHERE sound_type = ?
        ORDER BY timestamp DESC
        LIMIT ?
        ''', (sound_type, limit))

        detections = cursor.fetchall()
        conn.close()

        return detections

    def mark_as_processed(self, detection_id):
        """Mark a detection as processed"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
        UPDATE detections
        SET processed = 1
        WHERE id = ?
        ''', (detection_id,))

        conn.commit()
        conn.close()

    def store_movement(self, source_detection_id, speed, direction,
                       prediction_lat, prediction_lon, prediction_time):
        """Store movement analysis results"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute('''
        INSERT INTO movements
        (source_detection_id, speed, direction, prediction_latitude, 
         prediction_longitude, prediction_timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (source_detection_id, speed, direction, prediction_lat,
              prediction_lon, prediction_time))

        movement_id = cursor.lastrowid
        conn.commit()
        conn.close()
        logger.info(f"Movement analysis stored for detection {source_detection_id}")
        return movement_id


# Message Queue Manager
class MessageQueueManager:
    def __init__(self, config_path='config.ini'):
        self.config = self.load_config(config_path)
        self.connection = None
        self.channel = None
        self.exchange = self.config['exchange']
        self.detection_queue = self.config['detection_queue']
        self.ack_queue = self.config['acknowledgment_queue']
        self.alarm_queue = 'sensor_alarms'
        self.processed_queue = 'processed_data'
        self.connect()

    def load_config(self, config_path):
        """Load RabbitMQ configuration from INI file"""
        import configparser
        config = configparser.ConfigParser()
        config.read(config_path)

        # Get RabbitMQ configuration
        if 'RABBITMQ' in config:
            rabbitmq_config = {
                'host': config['RABBITMQ'].get('host', 'localhost'),
                'port': config['RABBITMQ'].getint('port', 5672),
                'virtual_host': config['RABBITMQ'].get('virtual_host', '/'),
                'user': config['RABBITMQ'].get('user', 'guest'),
                'password': config['RABBITMQ'].get('password', 'guest'),
                'exchange': config['RABBITMQ'].get('exchange', 'sound_monitoring'),
                'detection_queue': config['RABBITMQ'].get('detection_queue', 'sensor_detections'),
                'acknowledgment_queue': config['RABBITMQ'].get('acknowledgment_queue', 'sensor_acknowledgments')
            }
        else:
            # Default configuration
            rabbitmq_config = {
                'host': 'localhost',
                'port': 5672,
                'virtual_host': '/',
                'user': 'guest',
                'password': 'guest',
                'exchange': 'sound_monitoring',
                'detection_queue': 'sensor_detections',
                'acknowledgment_queue': 'sensor_acknowledgments'
            }

        logger.info(f"Loaded RabbitMQ configuration: {rabbitmq_config}")
        return rabbitmq_config

    def connect(self):
        """Connect to RabbitMQ server"""
        try:
            credentials = pika.PlainCredentials(
                self.config['user'],
                self.config['password']
            )

            parameters = pika.ConnectionParameters(
                host=self.config['host'],
                port=self.config['port'],
                virtual_host=self.config['virtual_host'],
                credentials=credentials
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Declare exchange
            self.channel.exchange_declare(
                exchange=self.exchange,
                exchange_type='topic',
                durable=True
            )

            # Declare queues
            self.channel.queue_declare(queue=self.detection_queue, durable=True)
            self.channel.queue_declare(queue=self.ack_queue, durable=True)
            self.channel.queue_declare(queue=self.alarm_queue, durable=True)
            self.channel.queue_declare(queue=self.processed_queue, durable=True)

            # Bind queues to exchange
            self.channel.queue_bind(
                exchange=self.exchange,
                queue=self.detection_queue,
                routing_key='detections'
            )

            self.channel.queue_bind(
                exchange=self.exchange,
                queue=self.ack_queue,
                routing_key='acknowledgments'
            )

            logger.info("Connected to RabbitMQ server")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def send_ack(self, sensor_id, packet_id):
        """Send acknowledgment back to the sensor"""
        ack_message = json.dumps({
            "sensor_id": sensor_id,
            "packet_id": packet_id,
            "status": "acknowledged",
            "timestamp": int(time.time())
        })

        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key='acknowledgments',
            body=ack_message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type='application/json',
                correlation_id=packet_id
            )
        )
        logger.info(f"Acknowledgment sent to sensor {sensor_id} for packet {packet_id}")

    def start_consuming(self, callback):
        """Start consuming messages from the detection queue"""
        self.channel.basic_consume(
            queue=self.detection_queue,
            on_message_callback=callback,
            auto_ack=False
        )
        logger.info(f"Started consuming messages from {self.detection_queue}")
        self.channel.start_consuming()

    def publish_processed_data(self, data):
        """Publish processed data to the processed_data queue"""
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key='processed_data',
            body=json.dumps(data),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type='application/json'
            )
        )
        logger.info("Published processed data")

    def publish_alarm(self, data):
        """Publish alarm to the sensor_alarms queue"""
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key='alarms',
            body=json.dumps(data),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type='application/json'
            )
        )
        logger.info("Published alarm")

    def close(self):
        """Close the connection"""
        if self.connection:
            self.connection.close()
            logger.info("Connection to RabbitMQ closed")


# Sound Detection Analyzer
class SoundAnalyzer:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def analyze_sound_characteristics(self, detection_data):
        """Analyze sound characteristics and identify type"""
        # In a real system, this would involve complex analysis
        # For this implementation, we'll just use the data provided by the sensor
        sound_type = detection_data["detection"]["type"]
        confidence = detection_data["detection"]["confidence"]

        # Basic verification of confidence
        if confidence < 0.3:
            sound_type = "unknown"

        return {
            "type": sound_type,
            "confidence": confidence,
            "characteristics": {
                "key_frequencies": detection_data["detection"].get("spectrum_key_frequencies", []),
                "amplitude": detection_data["detection"].get("amplitude", 0)
            }
        }


# Movement Analyzer
class MovementAnalyzer:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.earth_radius = 6371000  # Earth radius in meters

    def analyze_movement(self, sound_type):
        """Analyze movement of a sound source based on recent detections"""
        recent_detections = self.db_manager.get_recent_detections(sound_type, limit=5)

        if len(recent_detections) < 2:
            logger.info(f"Not enough detections to analyze movement for type {sound_type}")
            return None

        # Get the two most recent detections
        detection1 = recent_detections[1]  # Second most recent
        detection2 = recent_detections[0]  # Most recent

        # Extract data
        det1_id, det1_sensor, det1_time, det1_lat, det1_lon, det1_alt, det1_conf, det1_amp = detection1
        det2_id, det2_sensor, det2_time, det2_lat, det2_lon, det2_alt, det2_conf, det2_amp = detection2

        # Calculate distance between points (in meters)
        distance = self.calculate_distance(
            det1_lat, det1_lon, det1_alt,
            det2_lat, det2_lon, det2_alt
        )

        # Calculate time difference (in seconds)
        time_diff = abs(det2_time - det1_time)

        if time_diff == 0:
            logger.warning("Time difference is zero, skipping speed calculation")
            return None

        # Calculate speed (meters per second)
        speed = distance / time_diff

        # Calculate direction (azimuth in degrees)
        direction = self.calculate_bearing(
            det1_lat, det1_lon,
            det2_lat, det2_lon
        )

        # Predict next position
        prediction = self.predict_next_position(
            det2_lat, det2_lon, det2_time,
            speed, direction, time_diff
        )

        # Store movement analysis
        self.db_manager.store_movement(
            det2_id, speed, direction,
            prediction["latitude"], prediction["longitude"], prediction["timestamp"]
        )

        return {
            "detection_id": det2_id,
            "speed": speed,
            "direction": direction,
            "prediction": prediction
        }

    def calculate_distance(self, lat1, lon1, alt1, lat2, lon2, alt2):
        """Calculate the 3D distance between two points using Haversine formula"""
        # Convert latitude and longitude from degrees to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)

        # Haversine formula for the ground distance
        dlon = lon2_rad - lon1_rad
        dlat = lat2_rad - lat1_rad
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        ground_distance = self.earth_radius * c

        # Calculate the 3D distance including altitude
        altitude_diff = alt2 - alt1
        distance_3d = math.sqrt(ground_distance ** 2 + altitude_diff ** 2)

        return distance_3d

    def calculate_bearing(self, lat1, lon1, lat2, lon2):
        """Calculate the bearing (direction) from point 1 to point 2"""
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)

        dlon = lon2_rad - lon1_rad

        y = math.sin(dlon) * math.cos(lat2_rad)
        x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon)

        bearing = math.atan2(y, x)
        bearing = math.degrees(bearing)
        bearing = (bearing + 360) % 360  # Convert to 0-360 degrees

        return bearing

    def predict_next_position(self, lat, lon, timestamp, speed, direction, time_interval):
        """Predict the next position based on current position, speed, and direction"""
        # Predict for the same time interval as between previous detections
        prediction_time = timestamp + time_interval

        # Convert direction from degrees to radians
        direction_rad = math.radians(direction)

        # Calculate the distance that would be covered in the time interval
        distance = speed * time_interval

        # Convert to angular distance in radians
        angular_distance = distance / self.earth_radius

        # Convert latitude and longitude from degrees to radians
        lat_rad = math.radians(lat)
        lon_rad = math.radians(lon)

        # Calculate predicted position
        pred_lat_rad = math.asin(
            math.sin(lat_rad) * math.cos(angular_distance) +
            math.cos(lat_rad) * math.sin(angular_distance) * math.cos(direction_rad)
        )

        pred_lon_rad = lon_rad + math.atan2(
            math.sin(direction_rad) * math.sin(angular_distance) * math.cos(lat_rad),
            math.cos(angular_distance) - math.sin(lat_rad) * math.sin(pred_lat_rad)
        )

        # Convert back to degrees
        pred_lat = math.degrees(pred_lat_rad)
        pred_lon = math.degrees(pred_lon_rad)

        return {
            "latitude": pred_lat,
            "longitude": pred_lon,
            "timestamp": prediction_time
        }


# Main Processing Center
class ProcessingCenter:
    def __init__(self, config_path='config.ini'):
        self.config_path = config_path
        self.config = self.load_config()
        self.db_manager = DatabaseManager(config_path)
        self.queue_manager = MessageQueueManager(config_path)
        self.sound_analyzer = SoundAnalyzer(self.db_manager)
        self.movement_analyzer = MovementAnalyzer(self.db_manager)
        self.running = False

    def load_config(self):
        """Load configuration from INI file"""
        import configparser
        config = configparser.ConfigParser()
        config.read(self.config_path)

        # Get analysis configuration
        if 'ANALYSIS' in config:
            analysis_config = {
                'min_confidence_threshold': config['ANALYSIS'].getfloat('min_confidence_threshold', 0.7),
                'max_detection_age_seconds': config['ANALYSIS'].getint('max_detection_age_seconds', 300),
                'drone_speed_alarm_threshold': config['ANALYSIS'].getfloat('drone_speed_alarm_threshold', 20)
            }
        else:
            # Default configuration
            analysis_config = {
                'min_confidence_threshold': 0.7,
                'max_detection_age_seconds': 300,
                'drone_speed_alarm_threshold': 20
            }

        # Get server configuration
        if 'SERVER' in config:
            server_config = {
                'api_port': config['SERVER'].getint('api_port', 5000)
            }
        else:
            server_config = {
                'api_port': 5000
            }

        # Combine configurations
        combined_config = {**analysis_config, **server_config}

        logger.info(f"Loaded processing center configuration: {combined_config}")
        return combined_config

    def message_callback(self, ch, method, properties, body):
        """Callback function for received messages"""
        try:
            message = json.loads(body)
            logger.info(f"Received message: {message}")

            # Extract packet ID and other important info
            packet_id = message.get("packet_id")
            device_id = message.get("device_id")

            if not packet_id or not device_id:
                logger.error("Message missing required fields (packet_id or device_id)")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            # Store detection in database
            detection_id = self.db_manager.store_detection(message)

            # Send acknowledgment back to sensor
            self.queue_manager.send_ack(device_id, packet_id)

            # Record the acknowledgment in database
            self.db_manager.record_acknowledgment(packet_id, device_id)

            # Acknowledge message reception to RabbitMQ
            ch.basic_ack(delivery_tag=method.delivery_tag)

            # Analyze sound characteristics
            sound_analysis = self.sound_analyzer.analyze_sound_characteristics(message)

            # Get configuration
            config = self.load_config()
            min_confidence = config.get('min_confidence_threshold', 0.4)

            # Process movement if confidence is high enough
            if sound_analysis["confidence"] > min_confidence:
                # Run movement analysis in a separate thread to not block message processing
                threading.Thread(
                    target=self.process_movement,
                    args=(sound_analysis["type"],)
                ).start()

            # Mark detection as processed
            self.db_manager.mark_as_processed(detection_id)

            logger.info(f"Message processed successfully: {detection_id}")

        except json.JSONDecodeError:
            logger.error("Invalid JSON format in message")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def process_movement(self, sound_type):
        """Process movement analysis for a specific sound type"""
        try:
            movement_data = self.movement_analyzer.analyze_movement(sound_type)

            if movement_data:
                # Publish movement analysis results
                self.queue_manager.publish_processed_data({
                    "type": "movement_analysis",
                    "data": movement_data
                })

                # Get speed threshold from config
                speed_threshold = self.config.get('drone_speed_alarm_threshold', 20)

                # Check if speed is alarming
                if movement_data["speed"] > speed_threshold:
                    logger.warning(f"High speed object detected! {movement_data['speed']} m/s")

                    # Calculate speed in km/h for human readability
                    speed_kmh = movement_data["speed"] * 3.6

                    # Publish alarm
                    self.queue_manager.publish_alarm({
                        "type": "high_speed_detection",
                        "data": {
                            **movement_data,
                            "speed_kmh": speed_kmh,
                            "threshold_exceeded": True,
                            "threshold_value": speed_threshold,
                            "alarm_time": int(time.time())
                        }
                    })
        except Exception as e:
            logger.error(f"Error in movement analysis: {e}")

    def start(self):
        """Start the processing center"""
        if self.running:
            logger.warning("Processing center is already running")
            return

        self.running = True
        logger.info("Starting processing center")

        try:
            # Start consuming messages
            self.queue_manager.start_consuming(self.message_callback)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down")
            self.stop()
        except Exception as e:
            logger.error(f"Error in processing center: {e}")
            self.stop()

    def stop(self):
        """Stop the processing center"""
        if not self.running:
            return

        self.running = False
        logger.info("Stopping processing center")

        try:
            # Close connections
            self.queue_manager.close()
        except Exception as e:
            logger.error(f"Error while stopping: {e}")


# Simple API server for visualization
class APIServer:
    def __init__(self, processing_center, port=5000):
        self.processing_center = processing_center
        self.port = port

    def start(self):
        """Start the API server"""
        # In a real implementation, this would be a web server
        # For this example, we'll skip the actual implementation
        logger.info(f"API server started on port {self.port}")

        # This would be the main loop of the API server
        while self.processing_center.running:
            time.sleep(1)


# Main entry point
def main():
    # Create and start the processing center
    center = ProcessingCenter()

    # Start API server in a separate thread
    api_server = APIServer(center)
    api_thread = threading.Thread(target=api_server.start)
    api_thread.daemon = True
    api_thread.start()

    # Start processing center (this call is blocking)
    center.start()


if __name__ == "__main__":
    main()
