/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import database = require('models/database');
import appUrl = require('common/appUrl');
import changeSubscription = require('models/changeSubscription');
import changesCallback = require('common/changesCallback');
import commandBase = require('commands/commandBase');

class changesApi {

    private eventsId: string;
    source: EventSource;

    private allDocsHandlers = ko.observableArray<changesCallback<documentChangeNotificationDto>>();
    private allIndexesHandlers = ko.observableArray<changesCallback<indexChangeNotificationDto>>();
    private allBulkInsertsHandlers = ko.observableArray<changesCallback<bulkInsertChangeNotificationDto>>();
    private commandBase = new commandBase();

    constructor(private db: database) {
        this.eventsId = this.makeid();
        this.connect();
    }

    private connect() {
        if (!!window.EventSource) {
            var dbUrl = appUrl.forResourceQuery(this.db);

            //console.log("Connecting to changes API (db = " + this.db.name + ")");

            this.source = new EventSource(dbUrl + '/changes/events?id=' + this.eventsId);
            this.source.onmessage = (e) => this.onEvent(e);
            this.source.onerror = (e) => this.onError(e);

        } else {
            console.log("EventSource is not supported");
        }
    }

    private send(command: string, value?: string) {
        var args = {
            id: this.eventsId,
            command: command
        };
        if (value !== undefined) {
            args["value"] = value;
        }
        //TODO: exception handling?
        this.commandBase.query('/changes/config', args, this.db);
    }

    private onError(e: any) {
        this.commandBase.reportError('Changes stream was disconnected. Retrying connection shortly.');
    }

    private fireEvents<T>(events: Array<any>, param: T) {
        for (var i = 0; i < events.length; i++) {
            events[i].fire(param);
        }
    }

    private onEvent(e) {
        var json = JSON.parse(e.data);
        var type = json.Type;
        if (type === "Heartbeat") {
            // ignore 
        } else if (type === "DocumentChangeNotification") {
            this.fireEvents(this.allDocsHandlers(), json.Value);
        } else if (type === "IndexChangeNotification") {
            this.fireEvents(this.allIndexesHandlers(), json.Value);
        } else {
            console.log("Unhandled notification type: " + type);
        }
    }

    watchAllIndexes(onChange: (e: indexChangeNotificationDto) => void) {
        var callback = new changesCallback<indexChangeNotificationDto>(onChange);
        if (this.allIndexesHandlers().length == 0) {
            this.send('watch-indexes');
        }
        this.allIndexesHandlers.push(callback);
        return new changeSubscription(() => {
            this.allIndexesHandlers.remove(callback);
            if (this.allIndexesHandlers().length == 0) {
                this.send('unwatch-indexes');
            }
        });
    }

    watchAllDocs(onChange: (e: documentChangeNotificationDto) => void) {
        var callback = new changesCallback<documentChangeNotificationDto>(onChange);
        if (this.allDocsHandlers().length == 0) {
            this.send('watch-docs');
        }
        this.allDocsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allDocsHandlers.remove(callback);
            if (this.allDocsHandlers().length == 0) {
                this.send('unwatch-docs');
            }
        });
    }

    watchBulks(onChange: (e: bulkInsertChangeNotificationDto) => void) {
        var callback = new changesCallback<bulkInsertChangeNotificationDto>(onChange);
        if (this.allBulkInsertsHandlers().length == 0) {
            this.send('watch-bulk-operation');
        }
        this.allBulkInsertsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allDocsHandlers.remove(callback);
            if (this.allDocsHandlers().length == 0) {
                this.send('unwatch-bulk-operation');
            }
        });
    }

    watchDocPrefix(onChange: (e: documentChangeNotificationDto) => void, prefix?:string) {
        var callback = new changesCallback<documentChangeNotificationDto>(onChange);
        if (this.allDocsHandlers().length == 0) {
            this.send('watch-prefix', prefix);
        }
        this.allDocsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allDocsHandlers.remove(callback);
            if (this.allDocsHandlers().length == 0) {
                this.send('unwatch-prefix', prefix);
            }
        });
    }

    
    dispose() {
        if (this.source) {
            //console.log("Disconnecting from changes API");
            this.send('disconnect');
            this.source.close();
        }
    }

    private makeid() {
        var text = "";
        var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for (var i = 0; i < 5; i++)
            text += chars.charAt(Math.floor(Math.random() * chars.length));

        return text;
    }

}

export = changesApi;