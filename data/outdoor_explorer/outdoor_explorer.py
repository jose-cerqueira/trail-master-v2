var map = L.map('map').setView([latitude, longitude], 13);

// Tile layer for terrain
var terrainLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png');

// Vector layer for trails
var trailLayer = L.geoJSON(trailData);

// Control for layer toggling
L.control.layers({
    'Terrain': terrainLayer,
    'Trails': trailLayer
}).addTo(map);

// Add default layer to map
terrainLayer.addTo(map);
