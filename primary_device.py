import configparser
import json
import logging
import os
import sqlite3
import sys
import threading
import time
import uuid

import librosa
import numpy as np
import pika
import sounddevice as sd
from scipy.fft import fft
from scipy.io import wavfile
from scipy.signal import correlate

# Fix encoding issues for Windows console
if sys.platform == 'win32':
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr.reconfigure(encoding='utf-8')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("primary_device.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PrimaryDevice")


class SoundMonitoringDevice:
    """
    Primary sound monitoring device class
    """

    def __init__(self, config_file='config.ini'):
        """
        Initialize the device with configuration parameters
        """
        # Load configuration
        self.load_config(config_file)

        # Initialize database
        self.init_database()

        # Initialize reference signals
        self.reference_signals = {}
        self.load_reference_signals()

        # Initialize RabbitMQ connection
        self.init_rabbitmq()

        # Unacknowledged packet tracker
        self.unacknowledged_packets = {}

        # Running status
        self.is_running = False

        # Create debug directory
        os.makedirs("debug", exist_ok=True)

    def load_config(self, config_file):
        """
        Load parameters from configuration file
        """
        config = configparser.ConfigParser()

        # Create default config if file doesn't exist
        if not os.path.exists(config_file):
            config['DEVICE'] = {
                'device_id': f'SensorID-{uuid.uuid4().hex[:4]}',
                'location_latitude': '50.4501',
                'location_longitude': '30.5234',
                'location_altitude': '150'
            }

            config['RECORDING'] = {
                'sample_rate': '44100',
                'duration': '5',
                'processing_time': '1',
                'comparison_time': '1',
                'pause_time': '3',
                'channels': '1'
            }

            config['DETECTION'] = {
                'correlation_threshold': '0.5',  # Increased from 0.4
                'spectral_similarity_threshold': '50'  # Increased from 40
            }

            config['RABBITMQ'] = {
                'host': 'localhost',
                'port': '5672',
                'virtual_host': '/',
                'user': 'guest',
                'password': 'guest',
                'exchange': 'sound_monitoring',
                'detection_queue': 'sensor_detections',
                'acknowledgment_queue': 'sensor_acknowledgments'
            }

            config['DATABASE'] = {
                'device_db_file': 'primary_device.db'
            }

            with open(config_file, 'w') as configfile:
                config.write(configfile)

        # Load configuration
        config.read(config_file)

        # Device parameters
        self.device_id = config['DEVICE']['device_id']
        self.location = {
            'latitude': float(config['DEVICE']['location_latitude']),
            'longitude': float(config['DEVICE']['location_longitude']),
            'altitude': float(config['DEVICE']['location_altitude'])
        }

        # Recording parameters
        self.sample_rate = int(config['RECORDING']['sample_rate'])
        self.duration = int(config['RECORDING']['duration'])
        self.processing_time = int(config['RECORDING']['processing_time'])
        self.comparison_time = int(config['RECORDING']['comparison_time'])
        self.pause_time = int(config['RECORDING']['pause_time'])
        self.channels = int(config['RECORDING']['channels'])

        # Detection parameters
        self.correlation_threshold = float(config['DETECTION']['correlation_threshold'])
        self.spectral_similarity_threshold = float(config['DETECTION']['spectral_similarity_threshold'])

        # RabbitMQ parameters
        self.rabbitmq = {
            'host': config['RABBITMQ']['host'],
            'port': int(config['RABBITMQ']['port']),
            'virtual_host': config['RABBITMQ']['virtual_host'],
            'user': config['RABBITMQ']['user'],
            'password': config['RABBITMQ']['password'],
            'exchange': config['RABBITMQ']['exchange'],
            'detection_queue': config['RABBITMQ']['detection_queue'],
            'acknowledgment_queue': config['RABBITMQ']['acknowledgment_queue']
        }

        # Database parameters
        self.db_file = config['DATABASE']['device_db_file']

        logger.info(f"Configuration loaded for device {self.device_id}")

    def init_database(self):
        """
        Initialize local database for storing recordings and send status
        """
        try:
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.cursor = self.conn.cursor()

            # Create tables for recordings
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS recordings (
                id TEXT PRIMARY KEY,
                timestamp INTEGER,
                filename TEXT,
                processed BOOLEAN
            )
            ''')

            # Create table for detections
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS detections (
                id TEXT PRIMARY KEY,
                timestamp INTEGER,
                detection_type TEXT,
                confidence REAL,
                key_frequencies TEXT,
                amplitude REAL,
                acknowledged BOOLEAN DEFAULT 0,
                retry_count INTEGER DEFAULT 0,
                last_sent INTEGER
            )
            ''')

            self.conn.commit()
            logger.info("Database initialized")
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {e}")

    def preprocess_signal(self, signal, sample_rate=None):
        """
        Apply consistent preprocessing to both reference and recorded signals
        """
        # Ensure correct data type
        signal = signal.astype(np.float32)

        # Apply normalization
        if np.max(np.abs(signal)) > 0:
            signal = signal / np.max(np.abs(signal))

        return signal

    def load_reference_signals(self):
        """
        Optimized loading of reference signals with caching support
        """
        reference_dir = "references"
        cache_dir = "features_cache"

        # Ensure directories exist
        os.makedirs(reference_dir, exist_ok=True)
        os.makedirs(cache_dir, exist_ok=True)

        # Check for reference files
        files = [f for f in os.listdir(reference_dir) if f.endswith(".wav")]
        if not files:
            logger.warning("No reference signals found. Please add WAV files to the references/ directory")
            return

        # Load files with progress tracking
        logger.info(f"Loading {len(files)} reference signals...")

        for i, filename in enumerate(files):
            signal_type = os.path.splitext(filename)[0]
            filepath = os.path.join(reference_dir, filename)
            cache_path = os.path.join(cache_dir, f"{signal_type}.npz")

            try:
                # Check if cache exists and is newer than the source file
                use_cache = (os.path.exists(cache_path) and
                             os.path.getmtime(cache_path) > os.path.getmtime(filepath))

                if use_cache:
                    # Load from cache
                    cache = np.load(cache_path, allow_pickle=True)
                    self.reference_signals[signal_type] = {
                        'data': cache['data'],
                        'sample_rate': int(cache['sample_rate']),
                        'features': cache['features'].item()
                    }
                    logger.info(f"Loaded reference signal from cache ({i + 1}/{len(files)}): {signal_type}")
                else:
                    # Load audio directly
                    data, sample_rate = librosa.load(filepath, sr=None, mono=True)

                    # Preprocess signal
                    data = self.preprocess_signal(data)

                    # Store basic reference data
                    self.reference_signals[signal_type] = {
                        'data': data,
                        'sample_rate': sample_rate
                    }

                    # Extract features
                    features = self.extract_features(data, sample_rate)
                    self.reference_signals[signal_type]['features'] = features

                    # Save to cache
                    np.savez(
                        cache_path,
                        data=data,
                        sample_rate=sample_rate,
                        features=features
                    )

                    logger.info(f"Loaded reference signal ({i + 1}/{len(files)}): {signal_type}")

            except Exception as e:
                logger.error(f"Error loading reference signal {filename}: {e}")

        logger.info(f"Successfully loaded {len(self.reference_signals)} reference signals")

    def extract_features(self, audio_data, sample_rate):
        """
        Optimized feature extraction from audio data
        """
        features = {}

        # Use smaller FFT size for efficiency
        n_fft = min(2048, len(audio_data))
        fft_result = np.abs(fft(audio_data[:n_fft]))
        fft_freq = np.fft.fftfreq(n_fft, 1 / sample_rate)

        # Only use positive frequencies
        positive_freq_idx = np.where(fft_freq > 0)
        fft_result = fft_result[positive_freq_idx]
        fft_freq = fft_freq[positive_freq_idx]

        # Find peaks more efficiently
        threshold = 0.1 * np.max(fft_result)

        # Simplified peak detection
        if len(fft_result) > 2:  # Ensure there are enough points for peak detection
            is_peak = np.zeros(len(fft_result), dtype=bool)
            is_peak[1:-1] = (fft_result[1:-1] > fft_result[:-2]) & (fft_result[1:-1] > fft_result[2:])
            is_peak = is_peak & (fft_result > threshold)

            peak_indices = np.where(is_peak)[0]
            peaks = [(fft_freq[i], fft_result[i]) for i in peak_indices]

            # Sort by amplitude
            peaks.sort(key=lambda x: x[1], reverse=True)

            # Limit to top peaks
            peaks = peaks[:10]
        else:
            peaks = []

        # Key frequencies (only save frequencies, not amplitudes)
        features['key_frequencies'] = [peak[0] for peak in peaks[:5]]

        # MFCC calculation - simplified to avoid extreme values
        # Instead of using librosa.feature.mfcc which can lead to normalization issues,
        # use a simpler spectral centroid feature
        if len(audio_data) > 0:
            # Calculate spectral centroid as a simpler feature
            spectral_centroid = librosa.feature.spectral_centroid(y=audio_data, sr=sample_rate)
            features['mfcc'] = np.mean(spectral_centroid, axis=1)
        else:
            # Empty audio data case
            features['mfcc'] = np.array([0.0])

        return features

    def init_rabbitmq(self):
        """
        Initialize RabbitMQ connection
        """
        try:
            # Set up connection
            credentials = pika.PlainCredentials(
                self.rabbitmq['user'],
                self.rabbitmq['password']
            )
            parameters = pika.ConnectionParameters(
                host=self.rabbitmq['host'],
                port=self.rabbitmq['port'],
                virtual_host=self.rabbitmq['virtual_host'],
                credentials=credentials
            )
            self.connection = pika.BlockingConnection(parameters)

            # Channel for sending detections
            self.channel = self.connection.channel()

            # Declare exchange
            self.channel.exchange_declare(
                exchange=self.rabbitmq['exchange'],
                exchange_type='topic',
                durable=True
            )

            # Declare detections queue
            self.channel.queue_declare(
                queue=self.rabbitmq['detection_queue'],
                durable=True
            )

            # Bind detections queue to exchange
            self.channel.queue_bind(
                exchange=self.rabbitmq['exchange'],
                queue=self.rabbitmq['detection_queue'],
                routing_key='detections'
            )

            # Create a separate channel for acknowledgments
            self.ack_channel = self.connection.channel()

            # Declare acknowledgments queue (should match processing center's)
            self.ack_channel.queue_declare(
                queue=self.rabbitmq['acknowledgment_queue'],
                durable=True
            )

            # Bind acknowledgments queue to exchange
            self.ack_channel.queue_bind(
                exchange=self.rabbitmq['exchange'],
                queue=self.rabbitmq['acknowledgment_queue'],
                routing_key='acknowledgments'
            )

            # Set up acknowledgment consumer
            self.ack_channel.basic_consume(
                queue=self.rabbitmq['acknowledgment_queue'],
                on_message_callback=self.on_acknowledgment,
                auto_ack=True
            )

            logger.info("RabbitMQ connection established")

        except Exception as e:
            logger.error(f"Error setting up RabbitMQ: {e}")
            raise

    def listen_for_acknowledgments(self):
        """
        Listen for acknowledgments from processing center
        """
        try:
            logger.info("Started acknowledgment listener")
            self.ack_channel.start_consuming()
        except Exception as e:
            logger.error(f"Error in acknowledgment listener: {e}")

    def on_acknowledgment(self, ch, method, properties, body):
        """
        Process received acknowledgment from processing center
        """
        try:
            acknowledgment = json.loads(body)
            packet_id = acknowledgment.get('packet_id')

            if packet_id:
                logger.info(f"Received acknowledgment for packet: {packet_id}")

                # Remove from unacknowledged packets
                if packet_id in self.unacknowledged_packets:
                    del self.unacknowledged_packets[packet_id]

                # Update status in database
                self.cursor.execute(
                    "UPDATE detections SET acknowledged = 1 WHERE id = ?",
                    (packet_id,)
                )
                self.conn.commit()

        except Exception as e:
            logger.error(f"Error processing acknowledgment: {e}")

    def compare_signals(self, recorded_signal, sample_rate):
        """
        Enhanced comparison of recorded signal with reference signals
        Returns detected signal type and confidence level
        """
        if not self.reference_signals:
            logger.warning("No reference signals for comparison")
            return None

        # Save the recorded sample for debugging
        debug_file = f"debug/recorded_{int(time.time())}.wav"
        wavfile.write(debug_file, sample_rate, recorded_signal)
        logger.info(f"Saved recorded audio to {debug_file} for debugging")

        # Preprocess and extract features
        recorded_signal = self.preprocess_signal(recorded_signal)
        recorded_features = self.extract_features(recorded_signal, sample_rate)

        best_match = None
        best_confidence = 0

        # Debug info collector
        debug_info = []

        # Check each reference signal
        for signal_type, reference in self.reference_signals.items():
            # Save reference signal for comparison
            ref_debug_file = f"debug/{signal_type}_reference.wav"
            if not os.path.exists(ref_debug_file):
                wavfile.write(ref_debug_file, reference['sample_rate'], reference['data'])

            # Calculate sliding window correlation
            if len(reference['data']) > len(recorded_signal):
                # Reference signal is longer
                window_size = len(recorded_signal)
                step_size = window_size // 4  # 75% overlap for better detection

                correlation_results = []
                for i in range(0, len(reference['data']) - window_size + 1, step_size):
                    window = reference['data'][i:i + window_size]

                    # Normalize signals before correlation
                    norm_window = window / (np.sqrt(np.sum(window ** 2)) + 1e-10)
                    norm_recorded = recorded_signal / (np.sqrt(np.sum(recorded_signal ** 2)) + 1e-10)

                    # Calculate correlation
                    corr = np.max(correlate(norm_recorded, norm_window, mode='valid'))
                    correlation_results.append(corr)

                correlation = max(correlation_results) if correlation_results else 0

            elif len(recorded_signal) > len(reference['data']):
                # Recorded signal is longer
                window_size = len(reference['data'])
                step_size = window_size // 4

                correlation_results = []
                for i in range(0, len(recorded_signal) - window_size + 1, step_size):
                    window = recorded_signal[i:i + window_size]

                    # Normalize signals
                    norm_ref = reference['data'] / (np.sqrt(np.sum(reference['data'] ** 2)) + 1e-10)
                    norm_window = window / (np.sqrt(np.sum(window ** 2)) + 1e-10)

                    # Calculate correlation
                    corr = np.max(correlate(norm_window, norm_ref, mode='valid'))  # Fixed order of arguments
                    correlation_results.append(corr)

                correlation = max(correlation_results) if correlation_results else 0

            else:
                # Equal length signals
                norm_ref = reference['data'] / (np.sqrt(np.sum(reference['data'] ** 2)) + 1e-10)
                norm_recorded = recorded_signal / (np.sqrt(np.sum(recorded_signal ** 2)) + 1e-10)
                correlation = np.max(correlate(norm_recorded, norm_ref, mode='valid'))

            # Compare frequency features with increased tolerance
            ref_freqs = reference['features']['key_frequencies']
            rec_freqs = recorded_features['key_frequencies']

            # Skip if either list is empty
            if not ref_freqs or not rec_freqs:
                spectral_similarity = 0
            else:
                # Frequency matching with increased tolerance
                tolerance = 0.15  # 15% frequency tolerance
                matched_freqs = 0
                total_possible_matches = min(len(ref_freqs), len(rec_freqs))

                for rec_freq in rec_freqs:
                    for ref_freq in ref_freqs:
                        if abs(rec_freq - ref_freq) <= tolerance * ref_freq:
                            matched_freqs += 1
                            break

                # Calculate spectral similarity
                spectral_similarity = (matched_freqs / max(1, total_possible_matches)) * 100

            # Compare MFCC coefficients - simplified approach
            if len(recorded_features['mfcc']) == len(reference['features']['mfcc']):
                # Calculate a scaled similarity to avoid extreme values
                mfcc_similarity = 50  # Baseline similarity value
                mfcc_dist = np.linalg.norm(recorded_features['mfcc'] - reference['features']['mfcc'])

                # Scale distance to similarity in more controlled way
                if mfcc_dist < 1e-5:  # Almost identical
                    mfcc_similarity = 100
                elif mfcc_dist > 10:  # Very different
                    mfcc_similarity = 0
                else:
                    # Linear interpolation between 0 and 100
                    mfcc_similarity = max(0, 100 * (1 - mfcc_dist / 10))
            else:
                # Fallback if dimensions don't match
                mfcc_similarity = 0

            # Recalibrated confidence score - giving more weight to correlation and spectral
            confidence = 0.5 * correlation + 0.4 * (spectral_similarity / 100) + 0.1 * (mfcc_similarity / 100)

            # Collect debug info
            debug_info.append({
                'type': signal_type,
                'correlation': correlation,
                'spectral_similarity': spectral_similarity,
                'mfcc_similarity': mfcc_similarity,
                'confidence': confidence
            })

            # Update best match
            if confidence > best_confidence:
                best_confidence = confidence
                best_match = {
                    'type': signal_type,
                    'confidence': confidence,
                    'correlation': correlation,
                    'spectral_similarity': spectral_similarity,
                    'mfcc_similarity': mfcc_similarity,
                    'key_frequencies': recorded_features['key_frequencies'][:3] if len(
                        recorded_features['key_frequencies']) >= 3 else recorded_features['key_frequencies'],
                    'amplitude': np.max(np.abs(recorded_signal))
                }

        # Log detailed debug info
        logger.info("=== Sound Comparison Results ===")
        for info in sorted(debug_info, key=lambda x: x['confidence'], reverse=True):
            logger.info(f"Type: {info['type']}, " +
                        f"Correlation: {info['correlation']:.3f}, " +
                        f"Spectral: {info['spectral_similarity']:.2f}%, " +
                        f"MFCC: {info['mfcc_similarity']:.2f}%, " +
                        f"Confidence: {info['confidence']:.3f}")

        logger.info(f"Current thresholds: Correlation={self.correlation_threshold}, " +
                    f"Spectral={self.spectral_similarity_threshold}")

        # Modified detection logic - more lenient approach
        if best_match:
            # Consider match valid if either correlation OR spectral similarity is good
            # OR if the overall confidence is high enough
            matched = (best_match['correlation'] > self.correlation_threshold or
                       best_match['spectral_similarity'] > self.spectral_similarity_threshold or
                       best_match['confidence'] > 0.45)  # Added overall confidence threshold

            if matched:
                logger.info(f"Match found: {best_match['type']} with confidence {best_match['confidence']:.3f}")
                return best_match
            else:
                logger.info(f"Best match ({best_match['type']}) below thresholds")
                return None
        else:
            logger.info("No matches found")
            return None

    def record_audio(self):
        """
        Record audio from microphone
        """
        logger.info(f"Starting audio recording for {self.duration} seconds")

        # Record audio
        audio_data = sd.rec(
            int(self.duration * self.sample_rate),
            samplerate=self.sample_rate,
            channels=self.channels,
            dtype='float32'
        )
        sd.wait()  # Wait for recording to complete

        # Convert to mono if needed
        if self.channels == 2:
            audio_data = np.mean(audio_data, axis=1)
        else:
            audio_data = audio_data.flatten()

        logger.info("Audio recording complete")

        # Save recording
        timestamp = int(time.time())
        recording_id = str(uuid.uuid4())
        filename = f"recordings/{timestamp}_{recording_id}.wav"

        # Ensure directory exists
        os.makedirs("recordings", exist_ok=True)

        # Save file
        wavfile.write(filename, self.sample_rate, audio_data)

        # Record in database
        self.cursor.execute(
            "INSERT INTO recordings (id, timestamp, filename, processed) VALUES (?, ?, ?, ?)",
            (recording_id, timestamp, filename, False)
        )
        self.conn.commit()

        return audio_data, self.sample_rate, recording_id, timestamp

    def send_detection(self, detection, timestamp, retry=False):
        """
        Send detection information to processing center
        """
        # Create unique packet ID
        packet_id = str(uuid.uuid4())

        # Build data packet
        packet = {
            "device_id": self.device_id,
            "timestamp": timestamp,
            "location": self.location,
            "detection": {
                "type": detection['type'],
                "confidence": float(detection['confidence']),
                "spectrum_key_frequencies": [float(f) for f in detection['key_frequencies']],
                "amplitude": float(detection['amplitude'])
            },
            "packet_id": packet_id
        }

        # Add retry information if this is a retry
        if retry:
            packet["retry"] = True
            packet["original_timestamp"] = timestamp

        # Save send information to database
        self.cursor.execute(
            """
            INSERT INTO detections 
            (id, timestamp, detection_type, confidence, key_frequencies, amplitude, acknowledged, retry_count, last_sent) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet_id,
                timestamp,
                detection['type'],
                detection['confidence'],
                json.dumps(detection['key_frequencies']),
                detection['amplitude'],
                False,
                1 if retry else 0,
                int(time.time())
            )
        )
        self.conn.commit()

        # Send message via RabbitMQ
        try:
            self.channel.basic_publish(
                exchange=self.rabbitmq['exchange'],
                routing_key='detections',
                body=json.dumps(packet),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent message
                    content_type='application/json',
                    correlation_id=packet_id
                )
            )

            # Add packet to unacknowledged list
            self.unacknowledged_packets[packet_id] = {
                'timestamp': timestamp,
                'sent_time': time.time(),
                'detection': detection,
                'packet': packet
            }

            logger.info(f"Sent detection message: {packet_id}")

            return packet_id

        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return None

    def check_unacknowledged_packets(self):
        """
        Check unacknowledged packets and resend if necessary
        """
        current_time = time.time()
        packets_to_retry = []

        for packet_id, packet_info in list(self.unacknowledged_packets.items()):
            # If more than 5 seconds since sending
            if current_time - packet_info['sent_time'] > 5:
                logger.warning(f"No acknowledgment for packet {packet_id}, preparing to resend")
                packets_to_retry.append(packet_info)

                # Remove from current unacknowledged list
                del self.unacknowledged_packets[packet_id]

        # Resend packets
        for packet_info in packets_to_retry:
            self.send_detection(
                packet_info['detection'],
                packet_info['timestamp'],
                retry=True
            )

    def run_monitoring(self):
        """
        Start monitoring loop
        """
        self.is_running = True

        try:
            logger.info(f"Starting monitoring on device {self.device_id}")

            ack_thread = threading.Thread(target=self.listen_for_acknowledgments)
            ack_thread.daemon = True  # Make thread terminate when main thread exits
            ack_thread.start()

            while self.is_running:
                # Record audio
                audio_data, sample_rate, recording_id, timestamp = self.record_audio()

                logger.info("Processing recording...")

                # Compare with reference signals
                detection = self.compare_signals(audio_data, sample_rate)

                # If match found
                if detection:
                    logger.info(
                        f"Detected sound pollution type '{detection['type']}' with confidence {detection['confidence']:.3f}")

                    # Send detection information
                    self.send_detection(detection, timestamp)
                else:
                    logger.info("No matches detected")

                # Check unacknowledged packets
                self.check_unacknowledged_packets()

                # Add a small pause before next recording
                time.sleep(self.pause_time)

        except KeyboardInterrupt:
            logger.info("Monitoring stopped by user")
        except Exception as e:
            logger.error(f"Error during monitoring: {e}")
        finally:
            self.is_running = False
            self.cleanup()

    def save_debug_plots(self, recorded_signal, reference_signal, signal_type, sample_rate):
        """Save debug plots for visual comparison"""
        try:
            import matplotlib.pyplot as plt

            # Create debug directory
            debug_dir = "debug_plots"
            os.makedirs(debug_dir, exist_ok=True)

            # Time domain plot
            plt.figure(figsize=(12, 6))
            plt.subplot(2, 1, 1)
            plt.plot(recorded_signal, label='Recorded')
            plt.plot(reference_signal[:min(len(recorded_signal), len(reference_signal))], label='Reference')
            plt.legend()
            plt.title(f"Time Domain Comparison: {signal_type}")

            # Frequency domain plot
            plt.subplot(2, 1, 2)
            rec_fft = np.abs(fft(recorded_signal))
            ref_fft = np.abs(fft(reference_signal[:min(len(recorded_signal), len(reference_signal))]))

            min_len = min(len(rec_fft), len(ref_fft))
            freq = np.fft.fftfreq(min_len, 1 / sample_rate)

            plt.semilogy(freq[:min_len // 2], rec_fft[:min_len // 2], label='Recorded')
            plt.semilogy(freq[:min_len // 2], ref_fft[:min_len // 2], label='Reference')
            plt.legend()
            plt.title("Frequency Domain Comparison")

            # Save plot
            plt.tight_layout()
            plt.savefig(f"{debug_dir}/{signal_type}_comparison_{int(time.time())}.png")
            plt.close()

            logger.info(f"Saved comparison plot for {signal_type}")
        except Exception as e:
            logger.error(f"Error saving debug plots: {e}")

    def test_algorithm(self):
        """
        Test signal comparison with synthetic signals
        """
        logger.info("Running algorithm test with synthetic signals")

        # Save original reference signals
        original_reference_signals = self.reference_signals.copy()

        # Create synthetic reference
        t = np.linspace(0, 1, self.sample_rate)
        ref_signal = np.sin(2 * np.pi * 440 * t)  # 440 Hz sine wave

        # Create slightly modified version (should match)
        noise = 0.05 * np.random.normal(0, 1, len(t))
        test_signal1 = np.sin(2 * np.pi * 440 * t) + noise

        # Create completely different signal (should not match)
        test_signal2 = np.sin(2 * np.pi * 880 * t)  # 880 Hz sine wave

        # Extract features
        ref_features = self.extract_features(ref_signal, self.sample_rate)

        # Store reference
        self.reference_signals = {
            'test_440hz': {'data': ref_signal, 'sample_rate': self.sample_rate, 'features': ref_features}
        }

        # Test similar signal
        logger.info("Testing similar signal (should match):")
        result1 = self.compare_signals(test_signal1, self.sample_rate)

        # Test different signal
        logger.info("Testing different signal (should not match):")
        result2 = self.compare_signals(test_signal2, self.sample_rate)

        # Restore original reference signals after test
        self.reference_signals = original_reference_signals

        # Save debug files
        wavfile.write("debug/test_reference.wav", self.sample_rate, ref_signal)
        wavfile.write("debug/test_similar.wav", self.sample_rate, test_signal1)
        wavfile.write("debug/test_different.wav", self.sample_rate, test_signal2)

        return result1 is not None, result2 is not None

    def cleanup(self):
        """
        Close connections and resources
        """
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()

            if hasattr(self, 'connection') and self.connection and self.connection.is_open:
                self.connection.close()

            logger.info("Resources released")

        except Exception as e:
            logger.error(f"Error releasing resources: {e}")


# Function to simulate recording audio from file instead of microphone
def simulate_recording_from_file(device, filename):
    """
    Simulate recording sound signal from file
    """
    try:
        # Read WAV file
        sample_rate, audio_data = wavfile.read(filename)

        # Normalize
        if audio_data.dtype == np.int16:
            audio_data = audio_data.astype(np.float32) / 32768.0

        # Convert stereo to mono
        if len(audio_data.shape) > 1 and audio_data.shape[1] == 2:
            audio_data = np.mean(audio_data, axis=1)

        # Select random segment of length device.duration seconds
        if len(audio_data) > device.sample_rate * device.duration:
            start = np.random.randint(0, len(audio_data) - device.sample_rate * device.duration)
            audio_segment = audio_data[start:start + int(device.sample_rate * device.duration)]
        else:
            # If file is shorter, use all of it and pad with zeros
            audio_segment = np.zeros(int(device.sample_rate * device.duration))
            audio_segment[:len(audio_data)] = audio_data

        logger.info(f"Simulated recording from file {filename}")

        timestamp = int(time.time())
        recording_id = str(uuid.uuid4())

        return audio_segment, sample_rate, recording_id, timestamp

    except Exception as e:
        logger.error(f"Error simulating recording from file: {e}")
        return None, None, None, None


if __name__ == "__main__":
    # Check for reference signals directory
    if not os.path.exists("references"):
        os.makedirs("references")
        logger.info("Created directory for reference signals 'references/'")
        logger.info("Add reference WAV files to 'references/' directory before starting")

    # Create directory for recordings
    if not os.path.exists("recordings"):
        os.makedirs("recordings")
        logger.info("Created directory for recordings 'recordings/'")

    # Initialize device
    device = SoundMonitoringDevice()

    # Run a test to validate the algorithm
    try:
        logger.info("Running algorithm validation test...")
        similar_matched, different_matched = device.test_algorithm()
        if similar_matched and not different_matched:
            logger.info("Algorithm validation successful: matched similar signal, rejected different signal")
        else:
            logger.warning(
                f"Algorithm validation issues: similar signal matched: {similar_matched}, different signal matched: {different_matched}")
    except Exception as e:
        logger.error(f"Error during algorithm validation: {e}")

    # If no reference signals, show hint
    if not device.reference_signals:
        print("\n" + "=" * 80)
        print("WARNING! No reference signals found.")
        print("Please add WAV files with reference sounds to the 'references/' directory")
        print("The filename will be used as the signal type (e.g., 'drone.wav')")
        print("=" * 80 + "\n")

        # Ask if demo signals should be loaded
        try:
            choice = input("Would you like to create demonstration reference signals? (yes/no): ").strip().lower()
            if choice in ['так', 'yes', 'y', 't']:
                # Create demonstration reference signals
                from scipy.signal import chirp

                # Create directory if it doesn't exist
                os.makedirs("references", exist_ok=True)

                # Reference signal "drone"
                duration = 5.0
                t = np.linspace(0, duration, int(device.sample_rate * duration))

                # Drone signal: sum of several frequencies with modulation
                drone_signal = 0.5 * np.sin(2 * np.pi * 100 * t)  # Base frequency
                drone_signal += 0.3 * np.sin(2 * np.pi * 200 * t)  # Harmonic
                drone_signal += 0.2 * np.sin(2 * np.pi * 300 * t)  # Another harmonic
                # Add noise and amplitude modulation
                drone_signal *= (1 + 0.2 * np.sin(2 * np.pi * 5 * t))
                drone_signal += 0.1 * np.random.normal(0, 1, len(t))

                # Normalization
                drone_signal = 0.8 * drone_signal / np.max(np.abs(drone_signal))

                # Save drone signal
                wavfile.write("references/drone.wav", device.sample_rate, drone_signal.astype(np.float32))

                # Reference signal "siren"
                siren_signal = chirp(t, f0=500, f1=1000, t1=duration, method='linear')
                siren_signal = np.sin(2 * np.pi * 10 * t) * siren_signal  # Amplitude modulation
                siren_signal = 0.8 * siren_signal / np.max(np.abs(siren_signal))

                # Save siren signal
                wavfile.write("references/siren.wav", device.sample_rate, siren_signal.astype(np.float32))

                print("Created demonstration reference signals:")
                print("1. drone.wav - imitation of drone sound")
                print("2. siren.wav - imitation of siren sound")

                # Reload reference signals
                device.load_reference_signals()

        except KeyboardInterrupt:
            print("\nOperation cancelled")

    # Operating mode
    print("\nSelect operating mode:")
    print("1. Use microphone for real-time audio recording")
    print("2. Simulate recording from test WAV file")
    print("3. Debug mode - reduced thresholds for testing")

    try:
        mode = input("Enter mode number (1/2/3): ").strip()

        if mode == "3":
            # Debug mode with reduced thresholds
            print("\nStarting in DEBUG MODE with reduced thresholds")
            print("Current thresholds: Correlation=" +
                  f"{device.correlation_threshold}, Spectral={device.spectral_similarity_threshold}")

            # Reduce thresholds temporarily
            device.correlation_threshold = 0.3
            device.spectral_similarity_threshold = 30

            print(f"DEBUG thresholds: Correlation={device.correlation_threshold}, " +
                  f"Spectral={device.spectral_similarity_threshold}")

            # Ask for test file
            test_file = input("Enter path to test WAV file: ").strip()

            if not os.path.exists(test_file):
                print(f"File '{test_file}' not found.")
                sys.exit(1)

            # Run in simulation mode with debug settings
            print(f"\nStarting simulation with file: {test_file} in DEBUG mode")
            print("Press Ctrl+C to stop.")

            try:
                # Change standard recording method to simulation
                original_record_method = device.record_audio
                device.record_audio = lambda: simulate_recording_from_file(device, test_file)
                device.run_monitoring()
            finally:
                # Restore original method
                device.record_audio = original_record_method

        elif mode == "1":
            # Start monitoring with microphone
            print("\nStarting monitoring using microphone.")
            print("Press Ctrl+C to stop.")
            device.run_monitoring()

        elif mode == "2":
            # Simulation from file
            test_file = input("Enter path to test WAV file: ").strip()

            if not os.path.exists(test_file):
                print(f"File '{test_file}' not found.")

                # Offer to create test file
                choice = input("Would you like to create a demonstration test file? (yes/no): ").strip().lower()

                if choice in ['так', 'yes', 'y', 't']:
                    # Create test file with mixed signals
                    from scipy.signal import chirp

                    # Parameters
                    duration = 30.0  # 30 seconds for test file
                    t = np.linspace(0, duration, int(device.sample_rate * duration))

                    # Create background noise
                    background = 0.05 * np.random.normal(0, 1, len(t))

                    # Add drones at different times
                    test_signal = background.copy()

                    # Drone appears at 5th second
                    drone_start = int(5 * device.sample_rate)
                    drone_end = int(10 * device.sample_rate)
                    drone_t = t[drone_start:drone_end] - t[drone_start]

                    # Generate drone signal (as before)
                    drone_segment = 0.5 * np.sin(2 * np.pi * 100 * drone_t)
                    drone_segment += 0.3 * np.sin(2 * np.pi * 200 * drone_t)
                    drone_segment += 0.2 * np.sin(2 * np.pi * 300 * drone_t)
                    drone_segment *= (1 + 0.2 * np.sin(2 * np.pi * 5 * drone_t))
                    drone_segment += 0.1 * np.random.normal(0, 1, len(drone_t))

                    # Add with gradual rise and fade
                    fade_len = int(0.5 * device.sample_rate)
                    fade_in = np.linspace(0, 1, fade_len)
                    fade_out = np.linspace(1, 0, fade_len)

                    drone_segment[:fade_len] *= fade_in
                    drone_segment[-fade_len:] *= fade_out

                    # Add to overall signal
                    test_signal[drone_start:drone_end] += 0.8 * drone_segment

                    # Add siren at 15th second
                    siren_start = int(15 * device.sample_rate)
                    siren_end = int(20 * device.sample_rate)
                    siren_t = t[siren_start:siren_end] - t[siren_start]

                    # Generate siren signal
                    siren_duration = siren_t[-1] - siren_t[0]
                    siren_segment = chirp(siren_t, f0=500, f1=1000, t1=siren_duration, method='linear')
                    siren_segment = np.sin(2 * np.pi * 10 * siren_t) * siren_segment

                    # Add with gradual rise and fade
                    siren_segment[:fade_len] *= fade_in
                    siren_segment[-fade_len:] *= fade_out

                    # Add to overall signal
                    test_signal[siren_start:siren_end] += 0.8 * siren_segment

                    # Normalization
                    test_signal = 0.9 * test_signal / np.max(np.abs(test_signal))

                    # Save test file
                    test_file = "test_audio.wav"
                    wavfile.write(test_file, device.sample_rate, test_signal.astype(np.float32))

                    print(f"Created test file: {test_file}")
                    print("The file contains drone sounds (5-10 s) and siren sounds (15-20 s)")
                else:
                    print("No test file available. Exiting.")
                    sys.exit(1)

            # Simulation of monitoring from file
            print(f"\nStarting monitoring simulation from file: {test_file}")
            print("Press Ctrl+C to stop.")

            try:
                # Change standard recording method to simulation
                original_record_method = device.record_audio
                device.record_audio = lambda: simulate_recording_from_file(device, test_file)
                device.run_monitoring()
            finally:
                # Restore original method
                device.record_audio = original_record_method

        else:
            print("Invalid choice. Exiting.")

    except KeyboardInterrupt:
        print("\nProgram stopped by user.")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Clean up resources
        if 'device' in locals():
            device.cleanup()
