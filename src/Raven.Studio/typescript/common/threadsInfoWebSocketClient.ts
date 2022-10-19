/// <reference path="../../typings/tsd.d.ts" />
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");

class threadsInfoWebSocketClient extends abstractWebSocketClient<Raven.Server.Dashboard.ThreadsInfo> {

    private readonly onData: (data: Raven.Server.Dashboard.ThreadsInfo) => void;

    constructor(onData: (data: Raven.Server.Dashboard.ThreadsInfo) => void) {
        super(null);
        this.onData = onData;
    }

    get connectionDescription() {
        return "Threads Info";
    }

    protected webSocketUrlFactory() {
        return endpoints.global.threadsInfo.threadsInfoWatch;
    }

    get autoReconnect() {
        return true;
    }

    protected onMessage(e: Raven.Server.Dashboard.ThreadsInfo) {
        this.onData(e);
    }
}

export = threadsInfoWebSocketClient;

