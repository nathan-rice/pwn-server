import * as WebSocket from "ws";
import { Pool } from "pg";
import Cursor from "pg-cursor";
import _ from "lodash";

const log = console.log;

// TODO: Research better settings for the websocket server
const server = new WebSocket.Server({ port: 8080 });

const resources = {
  example: {
    connection: {
      host: "example.host",
      port: 5432,
      database: "example.database",
      max: 2
    },
    description: "example resource"
  }
};

server.on("connection", async ws => {
  ws.connections = {};
  ws.active = {};
  ws.on("message", async message => {
    log(message);
    const { resource, type, ...parameters } = JSON.parse(message);
    if (type === "execute") {
      const { statement, batchSize, queryId } = parameters;
      const connection = ws.connections[resource];
      if (connection) {
        try {
          if (batchSize) {
            const cursor = await connection.query(new Cursor(statement));
            ws.active[queryId] = true;
            let rowsRead;
            try {
              do {
                const start = new Date().getTime();
                cursor.read(batchSize, (rows, err) => {
                  if (err) {
                    throw err;
                  }
                  rowsRead = rows.length;
                  if (rowsRead) {
                    const duration = new Date().getTime() - start;
                    ws.send({
                      queryId,
                      duration,
                      success: true,
                      results: JSON.stringify(rows)
                    });
                  }
                });
              } while (rowsRead && ws.active[queryId]);
            } catch ({ message: error }) {
              const result = JSON.stringify({
                resource,
                statement,
                queryId,
                success: false,
                error
              });
              log(result);
              ws.send(result);
            }
          } else {
            const start = new Date().getTime();
            const results = await connection.query(statement);
            const duration = new Date().getTime() - start;
            ws.send(
              JSON.stringify({ queryId, duration, success: true, results: results.rows })
            );
          }
        } catch ({ message: error }) {
          const result = JSON.stringify({ queryId, success: false, error });
          log(result);
          ws.send(result);
        }
      }
    } else if (type === "abort") {
      const { queryId } = parameters;
      if (ws.active[queryId]) {
        delete ws.active[queryId];
        ws.send(JSON.stringify({ resource, type, success: true }));
      } else {
        ws.send(
          JSON.stringify({
            resource,
            type,
            success: false,
            error: `No active query with id ${queryId}`
          })
        );
      }
    } else if (type === "resources") {
      ws.send(
        JSON.stringify({
          success: true,
          results: _.map(resources, r => r.description)
        })
      );
    } else if (type === "open") {
      const { user, password } = parameters;
      if (ws.connections[resource]) {
        try {
          ws.connections[resource].close();
        } catch ({ message: error }) {
          const result = JSON.stringify({
            resource,
            type,
            success: false,
            error
          });
          log(result);
          ws.send(result);
        }
      }
      try {
        ws.connections[resource] = new Pool({
          ...resources[resource].connection,
          user,
          password
        });
        ws.send(JSON.stringify({ resource, type, success: true }));
      } catch ({ message: error }) {
        const result = JSON.stringify({
          resource,
          type,
          success: false,
          error
        });
        console.log(result);
        ws.send(result);
      }
    } else if (type === "close") {
      const { resource } = parameters;
      const connection = ws.connections[resource];
      if (connection) {
        try {
          connection.close();
          ws.send(JSON.stringify({ resource, type, success: true }));
        } catch ({ message: error }) {
          const result = JSON.stringify({ resource, type, error });
          console.log(result);
          ws.send(result);
        }
      }
    }
  });
});
