"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                t[p[i]] = s[p[i]];
        }
    return t;
};
var _this = this;
exports.__esModule = true;
var WebSocket = require("ws");
var pg_1 = require("pg");
var pg_cursor_1 = require("pg-cursor");
var lodash_1 = require("lodash");
var log = console.log;
// TODO: Research better settings for the websocket server
var server = new WebSocket.Server({ port: 8080 });
var resources = {
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
server.on("connection", function (ws) { return __awaiter(_this, void 0, void 0, function () {
    var _this = this;
    return __generator(this, function (_a) {
        ws.connections = {};
        ws.active = {};
        ws.on("message", function (message) { return __awaiter(_this, void 0, void 0, function () {
            var _a, resource, type, parameters, statement, batchSize, queryId_1, connection, cursor, rowsRead_1, _loop_1, error, result, start, results, duration, _b, error, result, queryId, user, password, error, result, error, result, resource_1, connection, error, result;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        log(message);
                        _a = JSON.parse(message), resource = _a.resource, type = _a.type, parameters = __rest(_a, ["resource", "type"]);
                        if (!(type === "execute")) return [3 /*break*/, 8];
                        statement = parameters.statement, batchSize = parameters.batchSize, queryId_1 = parameters.queryId;
                        connection = ws.connections[resource];
                        if (!connection) return [3 /*break*/, 7];
                        _c.label = 1;
                    case 1:
                        _c.trys.push([1, 6, , 7]);
                        if (!batchSize) return [3 /*break*/, 3];
                        return [4 /*yield*/, connection.query(new pg_cursor_1["default"](statement))];
                    case 2:
                        cursor = _c.sent();
                        ws.active[queryId_1] = true;
                        try {
                            _loop_1 = function () {
                                var start = new Date().getTime();
                                cursor.read(batchSize, function (rows, err) {
                                    if (err) {
                                        throw err;
                                    }
                                    rowsRead_1 = rows.length;
                                    if (rowsRead_1) {
                                        var duration = new Date().getTime() - start;
                                        ws.send({
                                            queryId: queryId_1,
                                            duration: duration,
                                            success: true,
                                            results: JSON.stringify(rows)
                                        });
                                    }
                                });
                            };
                            do {
                                _loop_1();
                            } while (rowsRead_1 && ws.active[queryId_1]);
                        }
                        catch (_d) {
                            error = _d.message;
                            result = JSON.stringify({
                                resource: resource,
                                statement: statement,
                                queryId: queryId_1,
                                success: false,
                                error: error
                            });
                            log(result);
                            ws.send(result);
                        }
                        return [3 /*break*/, 5];
                    case 3:
                        start = new Date().getTime();
                        return [4 /*yield*/, connection.query(statement)];
                    case 4:
                        results = _c.sent();
                        duration = new Date().getTime() - start;
                        ws.send(JSON.stringify({ queryId: queryId_1, duration: duration, success: true, results: results.rows }));
                        _c.label = 5;
                    case 5: return [3 /*break*/, 7];
                    case 6:
                        _b = _c.sent();
                        error = _b.message;
                        result = JSON.stringify({ queryId: queryId_1, success: false, error: error });
                        log(result);
                        ws.send(result);
                        return [3 /*break*/, 7];
                    case 7: return [3 /*break*/, 9];
                    case 8:
                        if (type === "abort") {
                            queryId = parameters.queryId;
                            if (ws.active[queryId]) {
                                delete ws.active[queryId];
                                ws.send(JSON.stringify({ resource: resource, type: type, success: true }));
                            }
                            else {
                                ws.send(JSON.stringify({
                                    resource: resource,
                                    type: type,
                                    success: false,
                                    error: "No active query with id " + queryId
                                }));
                            }
                        }
                        else if (type === "resources") {
                            ws.send(JSON.stringify({
                                success: true,
                                results: lodash_1["default"].map(resources, function (r) { return r.description; })
                            }));
                        }
                        else if (type === "open") {
                            user = parameters.user, password = parameters.password;
                            if (ws.connections[resource]) {
                                try {
                                    ws.connections[resource].close();
                                }
                                catch (_e) {
                                    error = _e.message;
                                    result = JSON.stringify({
                                        resource: resource,
                                        type: type,
                                        success: false,
                                        error: error
                                    });
                                    log(result);
                                    ws.send(result);
                                }
                            }
                            try {
                                ws.connections[resource] = new pg_1.Pool(__assign({}, resources[resource].connection, { user: user,
                                    password: password }));
                                ws.send(JSON.stringify({ resource: resource, type: type, success: true }));
                            }
                            catch (_f) {
                                error = _f.message;
                                result = JSON.stringify({
                                    resource: resource,
                                    type: type,
                                    success: false,
                                    error: error
                                });
                                console.log(result);
                                ws.send(result);
                            }
                        }
                        else if (type === "close") {
                            resource_1 = parameters.resource;
                            connection = ws.connections[resource_1];
                            if (connection) {
                                try {
                                    connection.close();
                                    ws.send(JSON.stringify({ resource: resource_1, type: type, success: true }));
                                }
                                catch (_g) {
                                    error = _g.message;
                                    result = JSON.stringify({ resource: resource_1, type: type, error: error });
                                    console.log(result);
                                    ws.send(result);
                                }
                            }
                        }
                        _c.label = 9;
                    case 9: return [2 /*return*/];
                }
            });
        }); });
        return [2 /*return*/];
    });
}); });
