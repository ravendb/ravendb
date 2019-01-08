/// <reference path="../../typings/tsd.d.ts" />

import endpoints = require("endpoints");

import abstractWebSocketClient = require("common/abstractWebSocketClient");

class runningQueriesWebSocketClient extends abstractWebSocketClient<Array<Raven.Server.Documents.Queries.LiveRunningQueriesCollector.ExecutingQueryCollection>> {

    private readonly onData: (items: Array<Raven.Server.Documents.Queries.LiveRunningQueriesCollector.ExecutingQueryCollection>) => void;
    
    constructor(onData: (items: Array<Raven.Server.Documents.Queries.LiveRunningQueriesCollector.ExecutingQueryCollection>) => void) {
        super(null);
        this.onData = onData;
    }

    protected get autoReconnect(): boolean {
        return true;
    }

    get connectionDescription() {
        return "Running Queries Client";
    }

    protected onMessage(items: Array<Raven.Server.Documents.Queries.LiveRunningQueriesCollector.ExecutingQueryCollection>) {
        this.onData(items);
    }

    protected webSocketUrlFactory() {
        return endpoints.global.serverWideQueriesDebug.debugQueriesRunningLive;
    }
}

export = runningQueriesWebSocketClient;

