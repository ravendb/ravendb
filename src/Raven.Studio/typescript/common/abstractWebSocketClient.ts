/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher");

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

    // it should return something like: /changes?foo=bar&singleUseAuthToken=123
    protected abstract webSocketUrlFactory(token: singleAuthToken): string;

    protected isHeartBeat(e: any): boolean {
        return !e.data.trim();
    }

    private connect(action: (token: singleAuthToken) => void, recoveringFromWebsocketFailure: boolean = false) {
        if (this.disposed) {
            if (!!this.connectToWebSocketTask)
                this.connectToWebSocketTask.resolve();
            return;
        }
        if (!recoveringFromWebsocketFailure) {
            this.connectToWebSocketTask = $.Deferred<void>();
        }

        new getSingleAuthTokenCommand(this.db)
            .execute()
            .done((tokenObject: singleAuthToken) => {
                action.call(this, tokenObject);
            })
            .fail((e) => {
                if (this.disposed) {
                    this.connectToWebSocketTask.reject();
                    return;
                }
                    
                const error = !!e.responseJSON ? e.responseJSON.Error : e.responseText;
                if (e.status === 0) {
                    // Connection has closed so try to reconnect every 3 seconds.
                    setTimeout(() => this.connect(action), 3 * 1000);
                }
                else if (e.status === ResponseCodes.ServiceUnavailable) {
                    // We're still loading the database, try to reconnect every 2 seconds.
                    setTimeout(() => this.connect(action, true), 2 * 1000);
                }
                else if (e.status !== ResponseCodes.Forbidden) { // authorized connection
                    messagePublisher.reportError(error || "Failed to connect to changes", e.responseText, e.StatusText);
                    this.connectToWebSocketTask.reject();
                }
            });
    }

    protected onClose(e: CloseEvent) {
        // empty
    }

    private connectWebSocket(token: singleAuthToken) {
        let connectionOpened: boolean = false;
        
        const wsProtocol = window.location.protocol === "https:" ? "wss://" : "ws://";
        const queryString = this.webSocketUrlFactory(token);
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

