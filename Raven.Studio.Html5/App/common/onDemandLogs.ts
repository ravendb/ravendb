/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import database = require('models/database');
import appUrl = require('common/appUrl');
import changeSubscription = require('models/changeSubscription');
import changesCallback = require('common/changesCallback');
import commandBase = require('commands/commandBase');
import folder = require("models/filesystem/folder");
import changesApi = require("common/changesApi");
import getSingleAuthTokenCommand = require("commands/getSingleAuthTokenCommand");
import shell = require("viewmodels/shell");
import onDemandLogsConfigureCommand = require("commands/onDemandLogsConfigureCommand");

class onDemandLogs {

    private eventsId: string;
    private resourcePath: string;
    public connectToLogsTask: JQueryDeferred<any>;
    private webSocket: WebSocket;
    private eventSource: EventSource;
    private readyStateOpen = 1;

    private onExitCallback: Function;

    private isCleanClose: boolean = false;
    private normalClosureCode = 1000;
    private normalClosureMessage = "CLOSE_NORMAL";
    static messageWasShownOnce: boolean = false;
    private successfullyConnectedOnce: boolean = false;

    private logEntryHandler:changesCallback<documentChangeNotificationDto>;

    private commandBase = new commandBase();

    constructor(private db: database, private onMessageCallback: Function) {
        this.eventsId = this.makeId();
        this.resourcePath = appUrl.forResourceQuery(this.db);
        this.connectToLogsTask = $.Deferred();

        if ("WebSocket" in window && changesApi.isServerSupportingWebSockets) {
            this.connect(this.connectWebSocket);
        }
        else
            if ("EventSource" in window) {
                 this.connect(this.connectEventSource);
        }
        else {
            //The browser doesn't support nor websocket nor eventsource
            //or we are in IE10 or IE11 and the server doesn't support WebSockets.
            //Anyway, at this point a warning message was already shown. 
            this.connectToLogsTask.reject();
        }
    }

    onExit(callback: Function) {
        this.onExitCallback = callback;
    }

    private connect(action: Function) {
        var getTokenTask = new getSingleAuthTokenCommand(this.db, true).execute();

        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                var token = tokenObject.Token;
                var connectionString = 'singleUseAuthToken=' + token + '&id=' + this.eventsId;

                action.call(this, connectionString);
            })
            .fail((e) => {
                this.connectToLogsTask.reject(e);
            });
    }

    private connectWebSocket(connectionString: string) {
        var connectionOpened: boolean = false;

        this.webSocket = new WebSocket('ws://' + window.location.host + this.resourcePath + '/admin/logs/events?' + connectionString);

        this.webSocket.onmessage = (e) => this.onMessage(e);
        this.webSocket.onerror = (e) => this.onError(e);
        this.webSocket.onopen = () => {
            console.log("Connected to WebSocket logs");
            this.successfullyConnectedOnce = true;
            connectionOpened = true;
            this.connectToLogsTask.resolve();
        }
    }

    private connectEventSource(connectionString: string) {
        var connectionOpened: boolean = false;

        this.eventSource = new EventSource(this.resourcePath + '/admin/logs/events?' + connectionString);

        this.eventSource.onmessage = (e) => this.onMessage(e);
        this.eventSource.onerror = (e) => {
            if (connectionOpened == false) {
                this.connectToLogsTask.reject();
            } else {
                this.onError(e);
            }
        };
        this.eventSource.onopen = () => { 
            this.successfullyConnectedOnce = true;
            connectionOpened = true;
            this.connectToLogsTask.resolve();
        }
    }

    private onError(e: Event) {
        this.eventSource.close();
        this.commandBase.reportError('On Demand Logs stream was disconnected.', "");
        if (this.onExitCallback) {
            this.onExitCallback();
        }
    }

    private showWarning(message: string, details: string) {
        if (changesApi.isServerSupportingWebSockets) {
            changesApi.isServerSupportingWebSockets = false;
            this.commandBase.reportWarning(message, details);
        }
    }

    private send(command: string, value?: string, needToSaveSentMessages: boolean = true) {
        this.connectToLogsTask.done(() => {
            var args = {
                id: this.eventsId,
                command: command
            };
            if (value !== undefined) {
                args["value"] = value;
            }

            //TODO: exception handling?
            this.commandBase.query('/admin/logs/configure', args, this.db);
        });
    }

    private fireEvents<T>(events: Array<any>, param: T, filter: (T) => boolean) {
        for (var i = 0; i < events.length; i++) {
            if (filter(param)) {
                events[i].fire(param);
            }
        }
    }

    private onMessage(e: any) {
        var eventDto: any = JSON.parse(e.data);
        if (!!eventDto.Type && eventDto.Type == 'Heartbeat') {
            return;
        }
        this.onMessageCallback(eventDto);
    }

    configureCategories(categoriesConfig: customLogEntryDto[]) {
        new onDemandLogsConfigureCommand(this.db, categoriesConfig, this.eventsId).execute();
    }

    dispose() {
        this.connectToLogsTask.done(() => {
            var isCloseNeeded: boolean;

            if (isCloseNeeded = this.webSocket && this.webSocket.readyState == this.readyStateOpen){
                console.log("Disconnecting from WebSocket Logs API");
                this.webSocket.close(this.normalClosureCode, this.normalClosureMessage);
            }
            else if (isCloseNeeded = this.eventSource && this.eventSource.readyState == this.readyStateOpen) {
                console.log("Disconnecting from EventSource Logs API");
                this.eventSource.close();
            }

            if (isCloseNeeded) {
                this.send('disconnect', undefined, false);
                this.isCleanClose = true;
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

export = onDemandLogs;