/// <reference path="../../typings/tsd.d.ts" />

import appUrl = require('common/appUrl');
import changeSubscription = require('common/changeSubscription');
import changesCallback = require('common/changesCallback');
import commandBase = require('commands/commandBase');
import idGenerator = require("common/idGenerator");
import adminLogsConfigureCommand = require("commands/database/debug/adminLogsConfigureCommand");

//TODO: use abstractWebSocketClient

class adminLogsClient {

    public connectionClosingTask: JQueryDeferred<any>;
    public connectionOpeningTask: JQueryDeferred<any>;
    private webSocket: WebSocket;
    private readyStateOpen = 1;
    private eventsId: string;
    private isCleanClose: boolean = false;
    private normalClosureCode = 1000;
    private normalClosureMessage = "CLOSE_NORMAL";
    private resourcePath: string;
    static messageWasShownOnce: boolean = false;
    private successfullyConnectedOnce: boolean = false;
    
    private commandBase = new commandBase();
    private adminLogsHandlers = ko.observableArray<changesCallback<logDto>>();

    constructor(private token: string) {
        this.eventsId = idGenerator.generateId();
        this.resourcePath = appUrl.baseUrl;
        this.connectionOpeningTask = $.Deferred();
        this.connectionClosingTask = $.Deferred();
    }

    public connect() {
        var connectionString = 'singleUseAuthToken=' + this.token + '&id=' + this.eventsId;
        if ("WebSocket" in window) {
            this.connectWebSocket(connectionString);
        } else {
            // The browser doesn't support websocket
            this.connectionOpeningTask.reject();
        }
    }

    private connectWebSocket(connectionString: string) {
        var connectionOpened: boolean = false;

        var wsProtocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
        this.webSocket = new WebSocket(wsProtocol + window.location.host + this.resourcePath + '/admin/logs/events?' + connectionString);

        this.webSocket.onmessage = (e) => this.onMessage(e);
        this.webSocket.onerror = (e) => {
            if (connectionOpened == false) {
                this.connectionOpeningTask.reject();
            } else {
                this.connectionClosingTask.resolve({ Error: e });
            }
        }
        this.webSocket.onclose = (e: CloseEvent) => {
            this.connectionClosingTask.resolve();
        }
        this.webSocket.onopen = () => {
            console.log("Connected to WebSocket admin logs");
            this.successfullyConnectedOnce = true;
            connectionOpened = true;
            this.connectionOpeningTask.resolve();
        }
    }

    private send(command: string, value?: string, needToSaveSentMessages: boolean = true) {
        this.connectionOpeningTask.done(() => {
            var args: any = {
                id: this.eventsId,
                command: command
            };
            if (value !== undefined) {
                args["value"] = value;
            }

            //TODO: exception handling?
            this.commandBase.query('/admin/logs/configure', args, null);
        });
    }

    private fireEvents<T>(events: Array<any>, param: T, filter: (obj: T) => boolean) {
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
        this.fireEvents(this.adminLogsHandlers(), eventDto, () => true);
    }


    watchAdminLogs(onChange: (e: logDto) => void) {
        var callback = new changesCallback<logDto>(onChange);
        this.adminLogsHandlers.push(callback);
        return new changeSubscription(() => {
            this.adminLogsHandlers.remove(callback);
        });
    }

    configureCategories(categoriesConfig: adminLogsConfigEntryDto[]) {
        new adminLogsConfigureCommand(null, categoriesConfig, this.eventsId).execute();
    }

    dispose() {
        this.connectionOpeningTask.done(() => {
            if (this.webSocket) {
                if (this.webSocket.readyState === this.readyStateOpen) {
                    console.log("Disconnecting from WebSocket Logs API");
                    this.webSocket.close(this.normalClosureCode, this.normalClosureMessage);
                }

                this.send('disconnect', undefined, false);
                this.isCleanClose = true;
            }
        });
    }
}

export = adminLogsClient;
