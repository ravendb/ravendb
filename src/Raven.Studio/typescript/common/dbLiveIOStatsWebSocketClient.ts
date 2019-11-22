/// <reference path="../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import endpoints = require("endpoints");
import liveIOStatsWebSocketClient = require("common/liveIOStatsWebSocketClient");

class dbLiveIOStatsWebSocketClient extends liveIOStatsWebSocketClient {

    constructor(db: database, 
                onData: (data: Raven.Server.Utils.IoMetrics.IOMetricsResponse) => void,
                dateCutOff?: Date) {
        super(db, onData, dateCutOff);
    }

    get connectionDescription() {
        return "Live I/O Stats";
    }

    protected webSocketUrlFactory() {
        return endpoints.databases.ioMetrics.debugIoMetricsLive;
    }
}

export = dbLiveIOStatsWebSocketClient;

