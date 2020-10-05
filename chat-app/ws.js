var server = 'ws://localhost:8080/';

function zPad(n) {
    if (n < 10) return '0' + n;
    else return n;
}

function formatDate(date) {
    return zPad(date.getHours()) + ':' + zPad(date.getMinutes()) + ':' + zPad(date.getSeconds());
}

function write_to_mbox(message, timestamp) {
    var line = '[' + timestamp + '] ' + message + '<br>';
    $('#messages').append(line);
}

$(document).ready(function() {
    $('#name').focus();

    $('#connect-form').submit(function() {
        var socket = new WebSocket(server);
        var name = $('#name').val();
        var chat_name = $('#chat-name').val();
        var msgTime = new Date();

        socket.onerror = function(error) {
            console.log('WebSocket Error: ' + error);
        };

        socket.onopen = function(event) {
            $('#jumbotron').hide();
            var date = new Date();
            write_to_mbox('Connected to: ' + server, formatDate(date));
            socket.send(name);
            socket.send(chat_name);
            $('#message_wrapper').show();
            $('#message').focus();
        };

        socket.onmessage = function(event) {
            write_to_mbox(event.data, formatDate(msgTime))
        };

        socket.onclose = function(event) {
            var date = new Date();
            write_to_mbox('Disconnected from ' + server, formatDate(date));
        };

        $('#message-form').submit(function() {
            msgTime = new Date();
            socket.send($('#message').val());
            socket.send(msgTime.getTime())
            $('#message').val('');
            return false;
        });

        return false;
    });
});
