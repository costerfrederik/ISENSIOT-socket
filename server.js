const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const { Pool, Client } = require('pg');

// Script settings
dashboard_url = 'http://localhost:8080';
socket_server_port = 3000;

const app = express();
const server = http.createServer(app);
const io = socketIO(server, {
    cors: {
        origin: dashboard_url,
    },
});

// PostgreSQL database settings
const pool = new Pool({
    user: 'isensiot',
    host: 'localhost',
    database: 'isensiot',
    password: 'password',
    port: 5432,
});

async function getNewData() {
    try {
        const client = await pool.connect();
        const query = `
            SELECT DISTINCT ON (mv.identifier) mv.identifier, p.*
            FROM my_vehicles mv
            LEFT JOIN positions p ON mv.identifier = p.identifier
            ORDER BY mv.identifier, p.id DESC
        `;

        const result = await client.query(query);
        const newData = result.rows.map(row => ({
            identifier: row.identifier,
            position: row.id ? (({ identifier, ...rest }) => rest)(row) : null
        }));

        client.release();
        return newData;
    } catch (error) {
        console.error('Something went wrong trying to get new data');
    }
}

pool.connect((err, client, done) => {
    if (err) {
        console.error('Error trying to connect to database');
        process.exit(1);
    }

    client.query('LISTEN refresh_needed');
    client.on('notification', async() => {
        const newData = await getNewData();
        io.emit('refresh_needed', newData);
    })
})

// When client connects to socket io server
io.on('connection', (socket) => {
    console.log(`user connect: ${socket.id}`);

    socket.on('disconnect', (reason) => {
        console.log(`user disconnected: ${socket.id}, reason: ${reason}`);
    });

    // Use once or on
    socket.once('data_request', async () => {
        console.log('Data is requested');
        const newData = await getNewData();
        socket.emit('refresh_needed', newData);
    });
});

// Listen on user defined port
server.listen(socket_server_port, () => {
    console.log(`WebSocket Server is listening on port ${socket_server_port}`);
});


