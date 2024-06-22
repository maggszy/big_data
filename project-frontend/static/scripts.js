$(function() {
    $('#datepicker').datepicker({
        dateFormat: 'yy-mm-dd',
        showButtonPanel: true,
        changeMonth: true,
        changeYear: true,
        yearRange: "-100:+0"
    });

    $('#timepicker').timepicker({
        scrollbar: true
    });

    $('#locationForm').on('submit', function(event) {
        var date = $('#datepicker').val();
        var time = $('#timepicker').val();
        var latitude = $('#latitude').val();
        var longitude = $('#longitude').val();

        if (!date) {
            alert('Please select a date.');
            event.preventDefault();
        } else if (!time) {
            alert('Please select a time.');
            event.preventDefault();
        } else if (!latitude || !longitude) {
            alert('Please select a location on the map.');
            event.preventDefault();
        } else {
            $('#loader').show(); // Show loader when form is submitted
        }
    });
});

var map = L.map('map').setView([51.505, -0.09], 2);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
}).addTo(map);

var marker;

map.on('click', function(e) {
    var lat = e.latlng.lat;
    var lng = e.latlng.lng;

    if (marker) {
        marker.setLatLng(e.latlng);
    } else {
        marker = L.marker(e.latlng).addTo(map);
    }

    $('#latitude').val(lat);
    $('#longitude').val(lng);
});
