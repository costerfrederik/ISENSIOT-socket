const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const { Pool } = require('pg');
const { booleanPointInPolygon } = require('@turf/turf');

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
        return await createFence(data);
    } catch (error) {
        console.error(error.message);
    }
}

function formatMapData(data) {
    const newData = [];

    for (let i = 0; i < data.rows.length; i++) {
        const row = data.rows[i];

        const formattedRow = {
            identifier: row.identifier,
            position: row.id ? (({ vehicle_identifier, identifier, multi_polygon, ...rest }) => rest)(row) : null,
        };

        let trespassing = false;

        if (row.longitude && row.latitude && row.multi_polygon) {
            const longLat = [row.longitude, row.latitude];
            trespassing = !booleanPointInPolygon(longLat, row.multi_polygon);
        }

        Object.assign(formattedRow, {
            trespassing: trespassing,
        });

        newData.push(formattedRow);
    }
    return newData;
}

async function logTrespassers(allData) {
    const client = await pool.connect();

    for (let i = 0; i < allData.length; i++) {
        const mapDataObject = allData[i];

        if (!mapDataObject.position || !mapDataObject.trespassing) {
            continue;
        }

        try {
            const selectQuery = `
                SELECT *  FROM geofence_violations
                WHERE vehicle_identifier = $1 AND datetime = $2
                ORDER BY id DESC
                LIMIT 1
            `;
            const selectResult = await client.query(selectQuery, [mapDataObject.identifier, mapDataObject.position.datetime]);

            if (selectResult.rows.length !== 0) {
                continue;
            }

            const createQuery = `
                INSERT INTO geofence_violations (vehicle_identifier, latitude, longitude, datetime)
                VALUES ($1, $2, $3, $4)
            `;
            await client.query(createQuery, [
                mapDataObject.identifier,
                mapDataObject.position.latitude,
                mapDataObject.position.longitude,
                mapDataObject.position.datetime,
            ]);
        } catch (error) {
            throw error;
        }
    }
    client.release();
    console.log(`Successfully logged violation for vehicles`);
}

async function getAllTrespassers() {
    try {
        const client = await pool.connect();
        const query = `
            SELECT * FROM geofence_violations
            WHERE datetime > now() - interval '7 days' 
            ORDER BY id DESC
        `;

        const result = await client.query(query);
        const allData = result.rows;

        client.release();
        return allData;
    } catch (error) {
        console.error('Something went wrong trying to get trespassers');
    }
}

async function getAllMapData() {
    try {
        const client = await pool.connect();
        const query = `
            SELECT DISTINCT ON (mv.identifier) mv.identifier, p.*, g.multi_polygon
            FROM my_vehicles mv
            LEFT JOIN positions p ON mv.identifier = p.vehicle_identifier
            LEFT JOIN geofences g ON mv.identifier = g.vehicle_identifier
            ORDER BY mv.identifier, p.id DESC
        `;

        const result = await client.query(query);
        const allData = formatMapData(result);

        client.release();
        return allData;
    } catch (error) {
        console.error('Something went wrong trying to get new data');
    }
}

pool.connect((err, client, done) => {
    if (err) {
        console.error('Error trying to connect to database');
        process.exit(1);
    }

    client.query('LISTEN positions_changed');
    client.on('notification', async () => {
        console.log('REQUEST: DB wants to send new map data to clients');
        const allData = await getAllMapData();
        const allTrespassers = await getAllTrespassers();
        logTrespassers(allData);

        io.emit('refresh_needed', allData);
        io.emit('violations_request_response', allTrespassers);
    });

    client.query('LISTEN my_vehicles_changed');
    client.on('notification', async () => {
        console.log('REQUEST: DB wants to send new map data to clients');
        const allData = await getAllMapData();
        io.emit('refresh_needed', allData);
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
        const allData = await getAllMapData();
        socket.emit('refresh_needed', allData);
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
        const fence = await saveFence(data);

        if (!fence) {
            socket.emit('fence_redraw_response');
            return;
        }

        socket.emit('fence_redraw_response', fence.multi_polygon);
    });

    // Returns multiPolygon if requested by client
    socket.on('fence_redraw', async (identifier) => {
        console.log(`${socket.id}: Socket wants to redraw fence`);
        const fence = await getFence(identifier);

        if (!fence) {
            socket.emit('fence_redraw_response');
            return;
        }

        socket.emit('fence_redraw_response', fence.multi_polygon);
    });

    socket.on('violations_request', async () => {
        console.log(`${socket.id}: Socket wants trespassers`);
        const allData = await getAllTrespassers();
        socket.emit('violations_request_response', allData);
    });
});

// Listen on user defined port
server.listen(socket_server_port, () => {
    console.log(`WebSocket Server is listening on port ${socket_server_port}`);
});
