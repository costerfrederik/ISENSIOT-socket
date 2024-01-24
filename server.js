const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const { Pool } = require('pg');

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

async function createTaxi(newTaxi) {
    try {
        const alreadyExists = await taxiExists(newTaxi['identifier']);

        if (alreadyExists) {
            throw new Error('Vehicle with that identifier already exists. Please change the identifier to continue');
        }

        const client = await pool.connect();

        const createQuery = `
            INSERT INTO my_vehicles (identifier)
            VALUES ($1)
        `;
        await client.query(createQuery, [newTaxi['identifier']]);

        client.release();
        console.log(`Successfully created new taxi: ${newTaxi['identifier']}`);
        return {
            message: `Successfully created new taxi: ${newTaxi['identifier']}`,
            success: true,
        };
    } catch (error) {
        console.error(error.message);
        return {
            message: error.message,
            success: false,
        };
    }
}

async function taxiExists(identifier) {
    try {
        const client = await pool.connect();

        const selectQuery = `
            SELECT *
            FROM my_vehicles
            WHERE identifier = $1
            LIMIT 1
        `;
        const selectResult = await client.query(selectQuery, [identifier]);

        client.release();
        return !!selectResult.rows[0];
    } catch (error) {
        throw error;
    }
}

async function clearFence(identifier) {
    try {
        const client = await pool.connect();

        const deleteQuery = `
            DELETE FROM geofences
            WHERE vehicle_identifier = $1
        `;
        await client.query(deleteQuery, [identifier]);

        client.release();
    } catch (error) {
        throw error;
    }
}

async function createFence(data) {
    try {
        const client = await pool.connect();

        const createQuery = `
            INSERT INTO geofences (vehicle_identifier, multi_polygon)
            VALUES ($1, $2)
        `;
        await client.query(createQuery, [data.identifier, data.multiPolygon]);

        const selectQuery = `
            SELECT *
            FROM geofences
            WHERE vehicle_identifier = $1
            LIMIT 1
        `;
        const selectResult = await client.query(selectQuery, [data.identifier]);
        const newFence = selectResult.rows[0];

        client.release();
        console.log(`Successfully created fence for vehicle: ${data.identifier}`);
        return newFence;
    } catch (error) {
        throw error;
    }
}
async function getFence(identifier) {
    try {
        const client = await pool.connect();

        const selectQuery = `
            SELECT * FROM geofences
            WHERE vehicle_identifier = $1
            LIMIT 1
        `;
        const selectResult = await client.query(selectQuery, [identifier]);
        const newFence = selectResult.rows[0];

        client.release();
        return newFence;
    } catch (error) {
        console.error(error.message);
    }
}

async function saveFence(data) {
    try {
        // Removes fence where vehicle_identifier
        await clearFence(data.identifier);
        const exists = await taxiExists(data.identifier);

        if (!exists || !data.multiPolygon || data.multiPolygon.coordinates.length === 0) {
            return;
        }

        // Creates new fence
        await createFence(data);
    } catch (error) {
        console.error(error.message);
    }
}

async function getNewData() {
    try {
        const client = await pool.connect();
        const query = `
            SELECT DISTINCT ON (mv.identifier) mv.identifier, p.*
            FROM my_vehicles mv
            LEFT JOIN positions p ON mv.identifier = p.vehicle_identifier
            ORDER BY mv.identifier, p.id DESC
        `;

        const result = await client.query(query);
        const newData = result.rows.map((row) => ({
            identifier: row.identifier,
            position: row.id ? (({ vehicle_identifier, identifier, ...rest }) => rest)(row) : null,
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
    client.on('notification', async () => {
        console.log('REQUEST: DB wants to send new map data to clients');
        const newData = await getNewData();
        io.emit('refresh_needed', newData);
    });
});

// When client connects to socket io server
io.on('connection', (socket) => {
    console.log(`user connect: ${socket.id}`);

    socket.on('disconnect', (reason) => {
        console.log(`user disconnected: ${socket.id}, reason: ${reason}`);
    });

    // Returns map data if request by client
    socket.once('data_request', async () => {
        console.log(`${socket.id}: Socket wants new map data`);
        const newData = await getNewData();
        socket.emit('refresh_needed', newData);
    });

    // Creates taxi
    socket.on('taxi_create', async (data) => {
        console.log(`${socket.id}: Socket wants to create a new taxi`);
        const response = await createTaxi(data);
        socket.emit('taxi_inserted', response);
    });

    // saves fence
    socket.on('fence_save', async (data) => {
        console.log(`${socket.id}: Socket wants to save fence`);
        await saveFence(data);
    });

    // Returns multiPolygon if requested by client
    socket.on('fence_redraw', async (identifier) => {
        console.log(`${socket.id}: Socket wants to redraw fence`);
        const newFence = await getFence(identifier);
        if (newFence) {
            socket.emit('fence_redraw_response', newFence.multi_polygon);
        }
    });
});

// Listen on user defined port
server.listen(socket_server_port, () => {
    console.log(`WebSocket Server is listening on port ${socket_server_port}`);
});
