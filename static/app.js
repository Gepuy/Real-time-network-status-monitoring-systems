
// Initialize map
let map = L.map('map').setView([50.4501, 30.5234], 10);  // Default to Kyiv coordinates

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
}).addTo(map);

// Markers for sensors and detections
let sensorMarkers = {};
let detectionMarkers = {};
let predictionMarkers = {};

// Refresh data periodically
function refreshData() {
    fetchStatistics();
    fetchDetections();
    fetchMovements();
}

// Fetch system statistics
function fetchStatistics() {
    fetch('/api/stats')
        .then(response => response.json())
        .then(data => {
            const statsContainer = document.getElementById('stats-container');
            
            let html = `
                <div class="stat-card">
                    <h3>Total Detections</h3>
                    <div class="value">${data.total_detections}</div>
                </div>
                <div class="stat-card">
                    <h3>Active Sensors</h3>
                    <div class="value">${data.total_sensors}</div>
                </div>
                <div class="stat-card">
                    <h3>Movement Tracks</h3>
                    <div class="value">${data.total_movements}</div>
                </div>
                <div class="stat-card">
                    <h3>Recent Activity</h3>
                    <div class="value">${data.recent_activity}</div>
                </div>
            `;
            
            // Add detection types
            let typesHtml = '<div class="stat-card"><h3>Detection Types</h3><div class="value">';
            for (const [type, count] of Object.entries(data.detection_types)) {
                typesHtml += `${type}: ${count}<br>`;
            }
            typesHtml += '</div></div>';
            
            html += typesHtml;
            statsContainer.innerHTML = html;
        })
        .catch(error => console.error('Error fetching statistics:', error));
}

// Fetch and display recent detections
function fetchDetections() {
    fetch('/api/detections?limit=10')
        .then(response => response.json())
        .then(data => {
            // Update detections list
            const detectionsContainer = document.getElementById('detections-container');
            let html = '';
            
            if (data.length === 0) {
                html = '<p>No detections yet.</p>';
            } else {
                data.forEach(detection => {
                    const date = new Date(detection.timestamp * 1000);
                    html += `
                        <div class="detection-item">
                            <strong>${detection.detection.type}</strong> (${(detection.detection.confidence * 100).toFixed(1)}%)<br>
                            Sensor: ${detection.sensor_id}<br>
                            Time: ${date.toLocaleString()}<br>
                            Amplitude: ${detection.detection.amplitude.toFixed(1)} dB
                        </div>
                    `;
                    
                    // Add marker to map
                    addDetectionMarker(detection);
                });
            }
            
            detectionsContainer.innerHTML = html;
        })
        .catch(error => console.error('Error fetching detections:', error));
}

// Fetch and display movement predictions
function fetchMovements() {
    fetch('/api/movements?limit=10')
        .then(response => response.json())
        .then(data => {
            // Update movements list
            const movementsContainer = document.getElementById('movements-container');
            let html = '';
            
            if (data.length === 0) {
                html = '<p>No movement predictions yet.</p>';
            } else {
                data.forEach(movement => {
                    const sourceDate = new Date(movement.source.timestamp * 1000);
                    const predictionDate = new Date(movement.prediction.timestamp * 1000);
                    
                    html += `
                        <div class="movement-item">
                            <strong>${movement.source.type}</strong><br>
                            Speed: ${movement.speed_kmh.toFixed(1)} km/h<br>
                            Direction: ${movement.direction.toFixed(1)}°<br>
                            From: ${sourceDate.toLocaleString()}<br>
                            Predicted at: ${predictionDate.toLocaleString()}
                        </div>
                    `;
                    
                    // Add prediction marker and path to map
                    addPredictionMarker(movement);
                });
            }
            
            movementsContainer.innerHTML = html;
        })
        .catch(error => console.error('Error fetching movements:', error));
}

// Add a detection marker to the map
function addDetectionMarker(detection) {
    // Remove old marker if exists
    if (detectionMarkers[detection.id]) {
        map.removeLayer(detectionMarkers[detection.id]);
    }
    
    // Create marker
    const marker = L.circleMarker([detection.location.latitude, detection.location.longitude], {
        radius: 8,
        fillColor: getColorForType(detection.detection.type),
        color: '#000',
        weight: 1,
        opacity: 1,
        fillOpacity: 0.8
    });
    
    // Add popup
    const date = new Date(detection.timestamp * 1000);
    marker.bindPopup(`
        <strong>${detection.detection.type}</strong><br>
        Confidence: ${(detection.detection.confidence * 100).toFixed(1)}%<br>
        Time: ${date.toLocaleString()}<br>
        Sensor: ${detection.sensor_id}
    `);
    
    // Add to map
    marker.addTo(map);
    detectionMarkers[detection.id] = marker;
}

// Add a prediction marker and path to the map
function addPredictionMarker(movement) {
    // Remove old prediction marker if exists
    if (predictionMarkers[movement.id]) {
        map.removeLayer(predictionMarkers[movement.id].marker);
        map.removeLayer(predictionMarkers[movement.id].path);
    }
    
    // Create source point
    const sourcePoint = [movement.source.latitude, movement.source.longitude];
    
    // Create prediction point
    const predictionPoint = [movement.prediction.latitude, movement.prediction.longitude];
    
    // Create path between points
    const path = L.polyline([sourcePoint, predictionPoint], {
        color: getColorForType(movement.source.type),
        weight: 2,
        opacity: 0.7,
        dashArray: '5, 5'
    });
    
    // Create marker for prediction
    const marker = L.circleMarker(predictionPoint, {
        radius: 8,
        fillColor: getColorForType(movement.source.type),
        color: '#000',
        weight: 1,
        opacity: 0.7,
        fillOpacity: 0.5
    });
    
    // Add popup
    const predictionDate = new Date(movement.prediction.timestamp * 1000);
    marker.bindPopup(`
        <strong>Predicted ${movement.source.type}</strong><br>
        Speed: ${movement.speed_kmh.toFixed(1)} km/h<br>
        Direction: ${movement.direction.toFixed(1)}°<br>
        Predicted arrival: ${predictionDate.toLocaleString()}
    `);
    
    // Add to map
    path.addTo(map);
    marker.addTo(map);
    
    // Store references
    predictionMarkers[movement.id] = {
        marker: marker,
        path: path
    };
}

// Get color based on detection type
function getColorForType(type) {
    const colors = {
        'drone': '#ff4136',
        'car': '#0074d9',
        'unknown': '#aaaaaa'
    };
    
    return colors[type] || '#aaaaaa';
}

// Initial data load
refreshData();

// Refresh data every 10 seconds
setInterval(refreshData, 10000);
            