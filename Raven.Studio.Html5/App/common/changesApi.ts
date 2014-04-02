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
    private dbUrl: string;

    private allDocsHandlers = ko.observableArray<changesCallback<documentChangeNotificationDto>>();
    private commandBase = new commandBase();

    constructor(private db: database) {
        this.eventsId = this.makeid();
        this.connect();
    }

    private connect() {
        if (!!window.EventSource) {

            // TODO appUrl.forDatabaseQuery(this.db)
            var dbUrl = "http://marcin-win:8081/databases/sample";
            this.dbUrl = dbUrl;
            //TODO: delete me

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
            args[value] = value;
        }
        //TODO: exception handling?
        this.commandBase.query('/changes/config', args, this.db);
    }

    private onError(e: any) {
        this.commandBase.reportError('Changes stream was disconnected. Retrying connection shortly.');
    }

    private onEvent(e) {
        var json = JSON.parse(e.data);
        var type = json.Type;
        if (type === "Heartbeat") {
            // ignore 
        } else if (type === "DocumentChangeNotification") {
            var handlers = this.allDocsHandlers();
            for (var i = 0; i < handlers.length; i++) {
                handlers[i].fire(json.Value);
            }
        } else {
            console.log("Unhandled notification type: " + type);
        }
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

    
    dispose() {
        if (this.source) {
            //TODO: send diconnect command!
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