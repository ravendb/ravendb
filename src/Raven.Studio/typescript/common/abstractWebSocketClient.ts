/// <reference path="../../typings/tsd.d.ts" />

import resource = require("models/resources/resource");
import appUrl = require("common/appUrl");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import messagePublisher = require("common/messagePublisher");

abstract class abstractWebSocketClient {

    private static readonly readyStateOpen = 1;

    private static messageWasShownOnce: boolean = false;

    connectToWebSocketTask: JQueryDeferred<void>;

    private readonly resourcePath: string;
    private webSocket: WebSocket;
    
    private isDisposing = false;
    private disposed: boolean = false;
    private isCleanClose: boolean = false;
    private successfullyConnectedOnce: boolean = false;
    private sentMessages: chagesApiConfigureRequestDto[] = [];
   
    protected constructor(protected rs: resource) {
        this.resourcePath = appUrl.forResourceQuery(this.rs);
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

    protected abstract onMessage(e: any): void;

    protected abstract get connectionDescription(): string;

    // it should return something like: /changes?foo=bar&singleUseAuthToken=123
    protected abstract webSocketUrlFactory(token: singleAuthToken): string;

    private connect(action: (token: singleAuthToken) => void, recoveringFromWebsocketFailure: boolean = false) {
        if (this.disposed) {
            if (!!this.connectToWebSocketTask)
                this.connectToWebSocketTask.resolve();
            return;
        }
        if (!recoveringFromWebsocketFailure) {
            this.connectToWebSocketTask = $.Deferred<void>();
        }

        new getSingleAuthTokenCommand(this.rs)
            .execute()
            .done((tokenObject: singleAuthToken) => {
                action.call(this, tokenObject);
            })
            .fail((e) => {
                if (this.isDisposing) {
                    this.connectToWebSocketTask.reject();
                    return;
                }
                    
                var error = !!e.responseJSON ? e.responseJSON.Error : e.responseText;
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

    protected onClose() {
        // empty by design
    }

    private connectWebSocket(token: singleAuthToken) {
        let connectionOpened: boolean = false;
        
        const wsProtocol = window.location.protocol === "https:" ? "wss://" : "ws://";
        const queryString = this.webSocketUrlFactory(token);
        const url = wsProtocol + window.location.host + this.resourcePath + queryString;
        this.webSocket = new WebSocket(url);

        this.webSocket.onmessage = (e) => this.onMessage(e);
        this.webSocket.onerror = (e) => {
            if (connectionOpened === false) {
                this.onError(e);
            }
        };
        this.webSocket.onclose = () => {
            this.onClose();
            if (this.isCleanClose === false) {
                // Connection has closed uncleanly, so try to reconnect.
                this.connect(this.connectWebSocket);
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
        this.successfullyConnectedOnce = true;
    }

    private reconnect() {
        if (this.successfullyConnectedOnce) {
            //TODO: don't send watch operations when server is restarted
            //send changes connection args after reconnecting
            this.sentMessages.forEach(args => this.send(args.Command, args.Param, false));
            
            if (abstractWebSocketClient.messageWasShownOnce) {
                messagePublisher.reportSuccess("Successfully reconnected to changes stream!");
                abstractWebSocketClient.messageWasShownOnce = false;
            }
        }
    }

    protected onError(e: Event) {
        if (abstractWebSocketClient.messageWasShownOnce === false) {
            messagePublisher.reportError("Changes stream was disconnected!", "Retrying connection shortly.");
            abstractWebSocketClient.messageWasShownOnce = true;
        }
    }

    //TODO: wait for confirmations! - using CommandId property - this method will be async!
    protected send(command: string, value?: string, needToSaveSentMessages: boolean = true) {
        this.connectToWebSocketTask.done(() => {
            var args: chagesApiConfigureRequestDto = {
                Command: command
            };
            if (value !== undefined) {
                args.Param = value;
            }

            const payload = JSON.stringify(args, null, 2);

            if (!this.closingOrClosed() || !this.isUnwatchCommand(command)) {
                this.webSocket.send(payload);
            }
                
            this.saveSentMessages(needToSaveSentMessages, command, args);
        });
    }

    private closingOrClosed() {
        const state = this.webSocket.readyState;
        return WebSocket.CLOSED === state || WebSocket.CLOSING === state;
    }

    private isUnwatchCommand(command: string) {
        return command.slice(0, 2) === "un";
    }

    private saveSentMessages(needToSaveSentMessages: boolean, command: string, args: chagesApiConfigureRequestDto) {
        if (needToSaveSentMessages) {
            if (this.isUnwatchCommand(command)) {
                var commandName = command.slice(2, command.length);
                this.sentMessages = this.sentMessages.filter(msg => msg.Command !== commandName);
            } else {
                this.sentMessages.push(args);
            }
        }
    }

    protected fireEvents<T>(events: Array<any>, param: T, filter: (element: T) => boolean) {
        for (let i = 0; i < events.length; i++) {
            if (filter(param)) {
                events[i].fire(param);
            }
        }
    }

    dispose() {
        this.isDisposing = true;
        this.disposed = true;
        this.connectToWebSocketTask.done(() => {
            if (this.webSocket && this.webSocket.readyState === abstractWebSocketClient.readyStateOpen) {
                console.log("Disconnecting from WebSocket (" + this.connectionDescription + ")");
                this.webSocket.close();
            }
        });
    }

    getResource() {
        return this.rs;
    }
}

export = abstractWebSocketClient;

