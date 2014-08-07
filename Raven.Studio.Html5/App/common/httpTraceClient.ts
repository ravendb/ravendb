 /// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import resource = require('models/resource');
import appUrl = require('common/appUrl');
import changeSubscription = require('models/changeSubscription');
import changesCallback = require('common/changesCallback');
import commandBase = require('commands/commandBase');
import folder = require("models/filesystem/folder");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");
import shell = require("viewmodels/shell");
import changesApi = require("common/changesApi");

class httpTraceClient {

    private resourcePath: string;
    public connectToChangesApiTask: JQueryDeferred<any>;
    private webSocket: WebSocket;
    static isServerSupportingWebSockets: boolean = true;
    private eventSource: EventSource;
    private readyStateOpen = 1;

    private isCleanClose: boolean = false;
    private normalClosureCode = 1000;
    private normalClosureMessage = "CLOSE_NORMAL";
    static messageWasShownOnce: boolean = false;
    private successfullyConnectedOnce: boolean = false;
    private sentMessages = [];
    private commandBase = new commandBase();
    private adminLogsHandlers = ko.observableArray<changesCallback<logNotificationDto>>();

    constructor(private rs?: resource, private token?:string) {
        
        this.resourcePath = !!rs ? appUrl.forResourceQuery(rs) : "";
        this.connectToChangesApiTask = $.Deferred();

        if ("WebSocket" in window && changesApi.isServerSupportingWebSockets) {
            this.connect(this.connectWebSocket);
        } else if ("EventSource" in window) {
            this.connect(this.connectEventSource);
        } else {
            //The browser doesn't support nor websocket nor eventsource
            //or we are in IE10 or IE11 and the server doesn't support WebSockets.
            //Anyway, at this point a warning message was already shown. 
            this.connectToChangesApiTask.reject();
        }
    }

    private connect(action: Function, needToReconnect: boolean = false) {
        var getTokenTask = new getSingleAuthTokenCommand(this.resourcePath, !this.rs).execute();

        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                var token = tokenObject.Token;
                var connectionString = 'singleUseAuthToken=' + token;

                action.call(this, connectionString);
            })
            .fail((e) => {

                if (e.status == 401) {
                    this.connectToChangesApiTask.reject(e);
                } else {
                    // Connection has closed so try to reconnect every 3 seconds.
                    setTimeout(() => this.connect(action), 3 * 1000);    
                }
            });
    }

    private connectWebSocket(connectionString: string) {
        var connectionOpened: boolean = false;

        this.webSocket = new WebSocket('ws://' + window.location.host + this.resourcePath + '/http-trace/websocket?' + connectionString);

        this.webSocket.onmessage = (e) => this.onMessage(e);
        this.webSocket.onerror = (e) => {
            if (connectionOpened == false) {
                this.serverNotSupportingWebsocketsErrorHandler();
            } else {
                this.onError(e);
            }
        };
        this.webSocket.onclose = (e: CloseEvent) => {
            if (this.isCleanClose == false && changesApi.isServerSupportingWebSockets) {
                // Connection has closed uncleanly, so try to reconnect.
                this.connect(this.connectWebSocket);
            }
        }
        this.webSocket.onopen = () => {
            console.log("Connected to WebSockets HTTP Trace " + ((!!this.rs && !!this.rs.name) ? ("for (rs = " + this.rs.name + ")") : "admin"));
            this.reconnect();
            this.successfullyConnectedOnce = true;
            connectionOpened = true;
            this.connectToChangesApiTask.resolve();
        }
    }

    private connectEventSource(connectionString: string) {
        var connectionOpened: boolean = false;

        this.eventSource = new EventSource(this.resourcePath + '/http-trace/events?' + connectionString);

        this.eventSource.onmessage = (e) => this.onMessage(e);
        this.eventSource.onerror = (e) => {
            if (connectionOpened == false) {
                this.connectToChangesApiTask.reject();
            } else {
                this.onError(e);
                this.eventSource.close();
                this.connect(this.connectEventSource);
            }
        };
        this.eventSource.onopen = () => {
            console.log("Connected to EventSource HTTP Trace " + ((!!this.rs && !!this.rs.name) ? ("for (rs = " + this.rs.name + ")") : "admin"));
            this.reconnect();
            this.successfullyConnectedOnce = true;
            connectionOpened = true;
            this.connectToChangesApiTask.resolve();
        }
    }

    private reconnect() {
        if (this.successfullyConnectedOnce) {
            ko.postbox.publish("HttpTraceReconnected", this.rs);

            if (changesApi.messageWasShownOnce) {
                this.commandBase.reportSuccess("Successfully reconnected to HTTP Trace stream!");
                changesApi.messageWasShownOnce = false;
            }
        }
    }

    private onError(e: Event) {
        if (changesApi.messageWasShownOnce == false) {
            this.commandBase.reportError('HTTP Trace stream was disconnected.', "Retrying connection shortly.");
            changesApi.messageWasShownOnce = true;
        }
    }

    private serverNotSupportingWebsocketsErrorHandler() {
        var warningMessage;
        var details;

        if ("EventSource" in window) {
            this.connect(this.connectEventSource);
            warningMessage = "Your server doesn't support the WebSocket protocol!";
            details = "EventSource API is going to be used instead. However, multi tab usage isn't supported. " +
                "WebSockets are only supported on servers running on Windows Server 2012 and equivalent.";
        } else {
            this.connectToChangesApiTask.reject();
            warningMessage = "Changes API is Disabled!";
            details = "Your server doesn't support the WebSocket protocol and your browser doesn't support the EventSource API. " +
                "In order to use it, please use a browser that supports the EventSource API.";
        }

        this.showWarning(warningMessage, details);
    }

    private showWarning(message: string, details: string) {
        if (changesApi.isServerSupportingWebSockets) {
            changesApi.isServerSupportingWebSockets = false;
            this.commandBase.reportWarning(message, details);
        }
    }

    private fireEvents<T>(events: Array<any>, param: T, filter: (T) => boolean) {
        for (var i = 0; i < events.length; i++) {
            if (filter(param)) {
                events[i].fire(param);
            }
        }
    }

    private onMessage(e: any) {
        var eventDto: changesApiEventDto = JSON.parse(e.data);
        var type = eventDto.Type;
        var value = eventDto.Value;

        if (type !== "Heartbeat") { // ignore heartbeat
            if (type === "LogNotification") {
                this.fireEvents(this.adminLogsHandlers(), value, (e) => true);
            }
            else {
                console.log("Unhandled Changes API notification type: " + type);
            }
        }
    }

    watchLogs(onChange: (e: logNotificationDto) => void) {
        var callback = new changesCallback<logNotificationDto>(onChange);
        this.adminLogsHandlers.push(callback);
        return new changeSubscription(() => {
            this.adminLogsHandlers.remove(callback);
        });
    }
    
    dispose() {
        this.connectToChangesApiTask.done(() => {
            if (this.webSocket && this.webSocket.readyState == this.readyStateOpen){
                console.log("Disconnecting from WebSocket HTTP Trace " + ((!!this.rs && !!this.rs.name) ? ("for (rs = " + this.rs.name + ")") : "admin"));
                this.isCleanClose = true;
                this.webSocket.close(this.normalClosureCode, this.normalClosureMessage);
            }
            else if (this.eventSource && this.eventSource.readyState == this.readyStateOpen) {
                console.log("Disconnecting from EventSource HTTP Trace " + ((!!this.rs && !!this.rs.name) ? ("for (rs = " + this.rs.name + ")") : "admin"));
                this.isCleanClose = true;
                this.eventSource.close();
            }
        });
    }

    private makeId() {
        var text = "";
        var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for (var i = 0; i < 5; i++)
            text += chars.charAt(Math.floor(Math.random() * chars.length));

        return text;
    }

}

export = httpTraceClient;