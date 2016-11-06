/// <reference path="../../typings/tsd.d.ts" />
import changeSubscription = require('common/changeSubscription');
import changesCallback = require('common/changesCallback');
import commandBase = require('commands/commandBase');
import idGenerator = require("common/idGenerator");

//TODO: use abstractWebSocketClient
class trafficWatchClient {
    
    public connectionOpeningTask: JQueryDeferred<any>;
    public connectionClosingTask: JQueryDeferred<any>;
    private webSocket: WebSocket;
    private readyStateOpen = 1;
    private eventsId:string;
    private isCleanClose: boolean = false;
    private normalClosureCode = 1000;
    private normalClosureMessage = "CLOSE_NORMAL";
    static messageWasShownOnce: boolean = false;
    private successfullyConnectedOnce: boolean = false;
    private sentMessages: string[] = [];
    private commandBase = new commandBase();
    private adminLogsHandlers = ko.observableArray<changesCallback<logNotificationDto>>();

    constructor(private resourcePath: string, private token:string) {
        this.connectionOpeningTask = $.Deferred();
        this.connectionClosingTask = $.Deferred();
        this.eventsId = idGenerator.generateId();
    }

    public connect() {
        var connectionString = 'singleUseAuthToken=' + this.token + '&id=' + this.eventsId;
        if ("WebSocket" in window) {
            this.connectWebSocket(connectionString);
        } else {
            //The browser doesn't support websocket
            this.connectionOpeningTask.reject();
        }
    }

    private connectWebSocket(connectionString: string) {
        var connectionOpened: boolean = false;

        var wsProtocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
        this.webSocket = new WebSocket(wsProtocol + window.location.host + this.resourcePath + '/traffic-watch/websocket?' + connectionString);

        this.webSocket.onmessage = (e) => this.onMessage(e);
        this.webSocket.onerror = (e) => {
            if (connectionOpened == false) {
                this.connectionOpeningTask.reject();
            } else {
                this.connectionClosingTask.resolve({Error:e});
            }
        };
        this.webSocket.onclose = (e: CloseEvent) => {
                this.connectionClosingTask.resolve();
        }
        this.webSocket.onopen = () => {
            console.log("Connected to WebSockets HTTP Trace for " + ((!!this.resourcePath) ? (this.resourcePath) : "admin"));
            this.successfullyConnectedOnce = true;
            connectionOpened = true;
            this.connectionOpeningTask.resolve();
        }
    }

    private fireEvents<T>(events: Array<any>, param: T, filter: (input: T) => boolean) {
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
                this.fireEvents(this.adminLogsHandlers(), value, () => true);
            }
            else {
                console.log("Unhandled Changes API notification type: " + type);
            }
        }
    }

    watchTraffic(onChange: (e: logNotificationDto) => void) {
        var callback = new changesCallback<logNotificationDto>(onChange);
        this.adminLogsHandlers.push(callback);
        return new changeSubscription(() => {
            this.adminLogsHandlers.remove(callback);
        });
    }
    
    disconnect() {
        this.connectionOpeningTask.done(() => {
            if (this.webSocket && this.webSocket.readyState === this.readyStateOpen){
                console.log("Disconnecting from WebSocket HTTP Trace for " + ((!!this.resourcePath) ? (this.resourcePath) : "admin"));
                this.isCleanClose = true;
                this.webSocket.close(this.normalClosureCode, this.normalClosureMessage);
            }
        });
    }
}

export = trafficWatchClient;
