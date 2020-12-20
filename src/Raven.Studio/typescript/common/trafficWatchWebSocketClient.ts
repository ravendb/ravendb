/// <reference path="../../typings/tsd.d.ts" />
import abstractWebSocketClient = require("common/abstractWebSocketClient");
import endpoints = require("endpoints");

class trafficWatchWebSocketClient extends abstractWebSocketClient<Raven.Client.Documents.Changes.TrafficWatchChange> {

    private readonly onData: (data: Raven.Client.Documents.Changes.TrafficWatchChange) => void;
    private readonly onConnectionStarted: () => void;
    private readonly onConnectionClosed: () => void;

    constructor(onData: (data: Raven.Client.Documents.Changes.TrafficWatchChange) => void,
                onConnectionEstablished: () => void,
                onConnectionClosed: () => void) {
        super(null);
        this.onData = onData;
        this.onConnectionStarted = onConnectionEstablished;
        this.onConnectionClosed = onConnectionClosed;
    }

    get connectionDescription() {
        return "Traffic Watch";
    }

    protected webSocketUrlFactory() {
        return endpoints.global.trafficWatch.adminTrafficWatch;
    }

    get autoReconnect() {
        return true;
    }

    protected onConnectionEstablished() {
        this.onConnectionStarted();
    }

    protected onClose() {
        this.onConnectionClosed();
    }
    
    protected onMessage(e: Raven.Client.Documents.Changes.TrafficWatchChange) {
        this.onData(e);
    }
}

export = trafficWatchWebSocketClient;
