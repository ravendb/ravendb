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

    private watchedPrefixes = {};

    private commandBase = new commandBase();

    constructor(private db: database) {
        this.eventsId = this.makeid();
        this.connect();
    }

    private connect() {
        if (!!window.EventSource) {
            var dbUrl = appUrl.forDatabaseQuery(this.db);

            console.log("Connecting to changes API (db = " + this.db.name + ")");

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
        if (typeof(value) !== "undefined") {
            args['value'] = value;
        }
        //TODO: exception handling?
        this.commandBase.query('/changes/config', args, this.db);
    }

    private onError(e: any) {
        this.commandBase.reportError('Changes stream was disconnected. Retrying connection shortly.');
    }

    private fireEvents<T>(events: Array<any>, param: T, filter: (T) => boolean) {
        for (var i = 0; i < events.length; i++) {
            if (filter(param)) {
                events[i].fire(param);
            }
        }
    }

    private onEvent(e) {
        var json = JSON.parse(e.data);
        var type = json.Type;
        if (type === "Heartbeat") {
            // ignore 
        } else if (type === "DocumentChangeNotification") {
            this.fireEvents(this.allDocsHandlers(), json.Value, (e) => true);
            for (var key in this.watchedPrefixes) {
                var callbacks = <KnockoutObservableArray<documentChangeNotificationDto>> this.watchedPrefixes[key];
                this.fireEvents(callbacks(), json.Value, (e) => e.Id != null && e.Id.match("^" + key));
            }
            
        } else if (type === "IndexChangeNotification") {
            this.fireEvents(this.allIndexesHandlers(), json.Value, (e) => true);
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

    watchDocsStartingWith(docIdPrefix: string, onChange: (e: documentChangeNotificationDto) => void): changeSubscription {
        var callback = new changesCallback<documentChangeNotificationDto>(onChange);
        if (typeof (this.watchedPrefixes[docIdPrefix]) === "undefined") {
            this.send('watch-prefix', docIdPrefix);
            this.watchedPrefixes[docIdPrefix] = ko.observableArray();
        }
        this.watchedPrefixes[docIdPrefix].push(callback);

        return new changeSubscription(() => {
            this.watchedPrefixes[docIdPrefix].remove(callback);
            if (this.watchedPrefixes[docIdPrefix].length == 0) {
                delete this.watchedPrefixes[docIdPrefix];
                this.send('unwatch-prefix', docIdPrefix);
            }
        });
    }
    
    dispose() {
        if (this.source) {
            console.log("Disconnecting from changes API");
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