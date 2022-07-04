/// <reference path="../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import endpoints = require("endpoints");
import liveIOStatsWebSocketClient = require("common/liveIOStatsWebSocketClient");
import appUrl from "common/appUrl";

class dbLiveIOStatsWebSocketClient extends liveIOStatsWebSocketClient {

    constructor(db: database, 
                location: databaseLocationSpecifier,
                onData: (data: Raven.Server.Utils.IoMetrics.IOMetricsResponse) => void,
                dateCutOff?: Date) {
        super(db, location, onData, dateCutOff);
    }

    get connectionDescription() {
        return "Live I/O Stats";
    }

    protected webSocketUrlFactory(location: databaseLocationSpecifier) {
        const args = appUrl.urlEncodeArgs(location);
        return endpoints.databases.ioMetrics.debugIoMetricsLive + args;
    }
}

export = dbLiveIOStatsWebSocketClient;

