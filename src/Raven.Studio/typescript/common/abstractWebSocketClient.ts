/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import appUrl = require("common/appUrl");

abstract class abstractWebSocketClient<T> {

    private static readonly readyStateOpen = 1;

    connectToWebSocketTask: JQueryDeferred<void>;

    protected readonly resourcePath: string;
    protected webSocket: WebSocket;
    
    protected disposed: boolean = false;

    protected abstract get autoReconnect(): boolean;
   
    protected constructor(protected db: database) {
        this.resourcePath = appUrl.forDatabaseQuery(this.db);
        this.connectToWebSocketTask = $.Deferred<void>();

        if ("WebSocket" in window) {
            this.connect(this.connectWebSocket);
        } else {
            //The browser doesn't support websocket
            //or we are in IE10 or IE11 and the server doesn't support WebSockets.
            //Anyway, at this point a warning message was already shown. 
            this.connectToWebSocketTask.reject();
        }
    }

    protected onMessage(e: T) {
        console.error("Unhandled message type: " + e);
    }

    protected abstract get connectionDescription(): string;

    // it should return something like: /changes?foo=bar
    protected abstract webSocketUrlFactory(): string;

    protected isHeartBeat(e: any): boolean {
        return !e.data.trim();
    }

    private connect(action: () => void, recoveringFromWebsocketFailure: boolean = false) {
        if (this.disposed) {
            if (!!this.connectToWebSocketTask)
                this.connectToWebSocketTask.resolve();
            return;
        }
        if (!recoveringFromWebsocketFailure) {
            this.connectToWebSocketTask = $.Deferred<void>();
        }

        action.call(this);
    }

    protected onClose(e: CloseEvent) {
        // empty
    }

    private connectWebSocket() {
        let connectionOpened: boolean = false;
        
        const wsProtocol = window.location.protocol === "https:" ? "wss://" : "ws://";
        const queryString = this.webSocketUrlFactory();
        const url = wsProtocol + window.location.host + this.resourcePath + queryString;
        this.webSocket = new WebSocket(url);

        this.webSocket.onmessage = (e) => {
            if (this.isHeartBeat(e)) {
                this.onHeartBeat();
            } else {
                this.onMessage(JSON.parse(e.data));
            }
        }
        this.webSocket.onerror = (e) => {
            if (connectionOpened === false) {
                this.onError(e);
            }
        };
        this.webSocket.onclose = (e: CloseEvent) => {
            this.onClose(e);
            if (!e.wasClean) {
                this.onError(e);

                if (this.autoReconnect) {
                    // Connection has closed uncleanly, so try to reconnect.
                    this.connect(this.connectWebSocket);
                }
            }
        }
        this.webSocket.onopen = () => {
            this.onOpen();
            connectionOpened = true;
        }
    }

    protected onOpen() {
        console.log("Connected to WebSocket changes API (" + this.connectionDescription + ")");
        
        this.reconnect();
        this.connectToWebSocketTask.resolve();
    }

    protected reconnect() {
        // empty by design
    }

    protected onError(e: Event) {
        // empty by design
    }

    protected onHeartBeat() {
        // empty by design
    }

    protected closingOrClosed() {
        const state = this.webSocket.readyState;
        return WebSocket.CLOSED === state || WebSocket.CLOSING === state;
    }

    protected fireEvents<T>(events: Array<any>, param: T, filter: (element: T) => boolean) {
        for (let i = 0; i < events.length; i++) {
            if (filter(param)) {
                events[i].fire(param);
            }
        }
    }

    dispose() {
        this.disposed = true;
        this.connectToWebSocketTask.done(() => {
            if (this.webSocket && this.webSocket.readyState === abstractWebSocketClient.readyStateOpen) {
                console.log("Disconnecting from WebSocket (" + this.connectionDescription + ")");
                this.webSocket.close();
            }
        });
    }

    getDatabase() {
        return this.db;
    }
}

export = abstractWebSocketClient;

