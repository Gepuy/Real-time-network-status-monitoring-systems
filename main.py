#!/usr/bin/env python3
# Sound Monitoring System - Main Application
# Implements the processing center for sound monitoring system
# Lab 4 - Digital Technologies in Energy
import json
import os
import sys
import time
import logging
import argparse
import threading
from flask import Flask, jsonify, request, render_template
from waitress import serve

# Import our processing center implementation
from sound_monitoring_center import ProcessingCenter, DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sound_monitoring.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('sound_monitoring')

# Flask application for API
app = Flask(__name__, template_folder='templates', static_folder='static')
processing_center = None
db_manager = None


# API routes
@app.route('/')
def index():
    """Render the dashboard page"""
    return render_template('index.html')


@app.route('/api/sensors', methods=['GET'])
def get_sensors():
    """Get all sensors"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()

    cursor.execute('''
    SELECT id, latitude, longitude, altitude, last_active
    FROM sensors
    ''')

    sensors = cursor.fetchall()
    conn.close()

    result = []
    for sensor in sensors:
        result.append({
            'id': sensor[0],
            'latitude': sensor[1],
            'longitude': sensor[2],
            'altitude': sensor[3],
            'last_active': sensor[4]
        })

    return jsonify(result)


@app.route('/api/detections', methods=['GET'])
def get_detections():
    """Get recent detections"""
    limit = request.args.get('limit', default=50, type=int)
    sound_type = request.args.get('type', default=None)

    conn = db_manager.get_connection()
    cursor = conn.cursor()

    if sound_type:
        cursor.execute('''
        SELECT d.id, d.sensor_id, d.timestamp, d.latitude, d.longitude, d.altitude, 
               d.sound_type, d.confidence, d.key_frequencies, d.amplitude, 
               s.id, s.latitude, s.longitude, s.altitude
        FROM detections d
        JOIN sensors s ON d.sensor_id = s.id
        WHERE d.sound_type = ?
        ORDER BY d.timestamp DESC
        LIMIT ?
        ''', (sound_type, limit))
    else:
        cursor.execute('''
        SELECT d.id, d.sensor_id, d.timestamp, d.latitude, d.longitude, d.altitude, 
               d.sound_type, d.confidence, d.key_frequencies, d.amplitude, 
               s.id, s.latitude, s.longitude, s.altitude
        FROM detections d
        JOIN sensors s ON d.sensor_id = s.id
        ORDER BY d.timestamp DESC
        LIMIT ?
        ''', (limit,))

    detections = cursor.fetchall()
    conn.close()

    result = []
    for det in detections:
        try:
            key_frequencies = json.loads(det[8])
        except:
            key_frequencies = []

        result.append({
            'id': det[0],
            'sensor_id': det[1],
            'timestamp': det[2],
            'location': {
                'latitude': det[3],
                'longitude': det[4],
                'altitude': det[5]
            },
            'detection': {
                'type': det[6],
                'confidence': det[7],
                'key_frequencies': key_frequencies,
                'amplitude': det[9]
            },
            'sensor': {
                'id': det[10],
                'latitude': det[11],
                'longitude': det[12],
                'altitude': det[13]
            }
        })

    return jsonify(result)


@app.route('/api/movements', methods=['GET'])
def get_movements():
    """Get movement predictions"""
    limit = request.args.get('limit', default=20, type=int)

    conn = db_manager.get_connection()
    cursor = conn.cursor()

    cursor.execute('''
    SELECT m.id, m.source_detection_id, m.speed, m.direction,
           m.prediction_latitude, m.prediction_longitude, m.prediction_timestamp,
           m.created_at, d.sound_type, d.latitude, d.longitude, d.timestamp
    FROM movements m
    JOIN detections d ON m.source_detection_id = d.id
    ORDER BY m.created_at DESC
    LIMIT ?
    ''', (limit,))

    movements = cursor.fetchall()
    conn.close()

    result = []
    for mov in movements:
        result.append({
            'id': mov[0],
            'source_detection_id': mov[1],
            'speed': mov[2],
            'speed_kmh': mov[2] * 3.6,  # Convert m/s to km/h
            'direction': mov[3],
            'prediction': {
                'latitude': mov[4],
                'longitude': mov[5],
                'timestamp': mov[6]
            },
            'created_at': mov[7],
            'source': {
                'type': mov[8],
                'latitude': mov[9],
                'longitude': mov[10],
                'timestamp': mov[11]
            }
        })

    return jsonify(result)


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get system statistics"""
    conn = db_manager.get_connection()
    cursor = conn.cursor()

    # Total detections
    cursor.execute('SELECT COUNT(*) FROM detections')
    total_detections = cursor.fetchone()[0]

    # Detections by type
    cursor.execute('''
    SELECT sound_type, COUNT(*) 
    FROM detections 
    GROUP BY sound_type
    ''')
    types = cursor.fetchall()
    detection_types = {t[0]: t[1] for t in types}

    # Total sensors
    cursor.execute('SELECT COUNT(*) FROM sensors')
    total_sensors = cursor.fetchone()[0]

    # Total movements analyzed
    cursor.execute('SELECT COUNT(*) FROM movements')
    total_movements = cursor.fetchone()[0]

    # Recent activity
    cursor.execute('''
    SELECT COUNT(*) FROM detections
    WHERE timestamp > ?
    ''', (int(time.time()) - 3600,))  # Last hour
    recent_activity = cursor.fetchone()[0]

    conn.close()

    return jsonify({
        'total_detections': total_detections,
        'detection_types': detection_types,
        'total_sensors': total_sensors,
        'total_movements': total_movements,
        'recent_activity': recent_activity
    })


def start_api_server(port):
    """Start the API server in production mode"""
    logger.info(f"Starting API server on port {port}")
    serve(app, host='0.0.0.0', port=port)


def setup_static_files():
    """Create basic HTML and JS files for the dashboard"""
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)

    # Skip file creation if index.html already exists
    if os.path.exists('templates/index.html'):
        logger.info("Using existing index.html template")
    else:
        logger.info("Creating default index.html template")
        with open('templates/index.html', 'w') as f:
            # Your original template code here
            pass

    # Create basic CSS if it doesn't exist
    if not os.path.exists('static/style.css'):
        logger.info("Creating default style.css")
        with open('static/style.css', 'w') as f:
            # Your original CSS code here
            pass

    # Create basic JavaScript if it doesn't exist
    if not os.path.exists('static/app.js'):
        logger.info("Creating default app.js")
        with open('static/app.js', 'w') as f:
            # Your original JavaScript code here
            pass


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Sound Monitoring System')
    parser.add_argument('--config', type=str, default='config.ini', help='Path to config file')
    parser.add_argument('--port', type=int, default=5000, help='API server port (overrides config file)')
    parser.add_argument('--debug', action='store_true', help='Run Flask in debug mode')
    args = parser.parse_args()

    # Global references (needed for Flask routes)
    global processing_center, db_manager

    try:
        # Initialize processing center
        processing_center = ProcessingCenter(config_path=args.config)
        db_manager = processing_center.db_manager

        # Create static files (but don't overwrite existing ones)
        setup_static_files()

        # Determine port
        port = args.port if args.port else processing_center.config.get('api_port', 5000)

        if args.debug:
            # Run Flask in debug mode
            logger.info(f"Starting Flask development server on port {port}")
            app.run(host='0.0.0.0', port=port, debug=True)
        else:
            # Start the API server in a separate thread
            api_thread = threading.Thread(target=start_api_server, args=(port,))
            api_thread.daemon = True
            api_thread.start()

            # Start the processing center
            processing_center.start()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
        if processing_center:
            processing_center.stop()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error in main: {e}")
        if processing_center:
            processing_center.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()