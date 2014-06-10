/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import database = require('models/database');
import appUrl = require('common/appUrl');
import changeSubscription = require('models/changeSubscription');
import changesCallback = require('common/changesCallback');
import commandBase = require('commands/commandBase');

class changesApi {

    private eventsId: string;
    private socket: WebSocket;

    private allDocsHandlers = ko.observableArray<changesCallback<documentChangeNotificationDto>>();
    private allIndexesHandlers = ko.observableArray<changesCallback<indexChangeNotificationDto>>();
    private allTransformersHandlers = ko.observableArray<changesCallback<transformerChangeNotificationDto>>();
    private watchedPrefixes = {};
    private allBulkInsertsHandlers = ko.observableArray<changesCallback<bulkInsertChangeNotificationDto>>();
    private commandBase = new commandBase();
    private eventQueueName: string;
    private lastEventQueueCheckTime = new Date().getTime();
    private pollQueueHandle = 0;

    private static eventQueuePollInterval = 4000;
    private static eventQueueOwnerDeadTime = 10000;

    constructor(private db: database) {
        this.eventsId = this.makeId();
        this.eventQueueName = "Raven/Studio/ChangesApiEventQueue_" + db.name;
        this.connect();
    }

    private connect() {
        
        if (!!window['WebSocket'] ) {
            var dbUrl = appUrl.forResourceQuery(this.db);
            console.log("Connecting to changes API (db = " + this.db.name + ")");
            var changesApiUrl = "ws://" + window.location.host + dbUrl + '/changes/websocket?id=' + this.eventsId;
            try {
                this.socket = new window['WebSocket'](changesApiUrl);
            }
            catch (error) {
                console.error("Unable to connect to changes API using " + changesApiUrl);
            }

            this.socket.onmessage = (e) => this.onMessage(e);
            this.socket.onerror = (e) => this.onError(e);

        } else {
            console.log("Web sockets are not supported in your browser. http://caniuse.com/#search=websocket");
        }
    }

    private disconnect() {
        this.send('disconnect');
        if (this.socket) {
            this.socket.close();
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

    private onMessage(e: any) {
        var eventDto: changesApiEventDto = JSON.parse(e.data);
        this.processEvent(eventDto);
    }

    private onError(e: any) {
        this.commandBase.reportError('Lost connection to the server. Retrying shortly.');
    }

    private fireEvents<T>(events: Array<any>, param: T, filter: (T) => boolean) {
        for (var i = 0; i < events.length; i++) {
            if (filter(param)) {
                events[i].fire(param);
            }
        }
    }

    private processEvent(change: changesApiEventDto) {
        if (change.Type === "Heartbeat") {
            // ignore 
        } else {
            if (change.Type === "DocumentChangeNotification") {
                this.fireEvents(this.allDocsHandlers(), change.Value, (e) => true);
                for (var key in this.watchedPrefixes) {
                    var callbacks = <KnockoutObservableArray<documentChangeNotificationDto>> this.watchedPrefixes[key];
                    this.fireEvents(callbacks(), change.Value, (e) => e.Id != null && e.Id.match("^" + key));
                }

            } else if (change.Type === "IndexChangeNotification") {
                this.fireEvents(this.allIndexesHandlers(), change.Value, (e) => true);
            } else if (change.Type === "TransformerChangeNotification") {
                this.fireEvents(this.allTransformersHandlers(), change.Value, (e) => true);
            } else if (change.Type === "BulkInsertChangeNotification") {
                this.fireEvents(this.allBulkInsertsHandlers(), change.Value, (e) => true);
            }
            else {
                console.log("Unhandled Changes API notification type: " + change.Type);
            }
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

    watchAllTransformers(onChange: (e: transformerChangeNotificationDto) => void) {
        var callback = new changesCallback<transformerChangeNotificationDto>(onChange);
        if (this.allTransformersHandlers().length == 0) {
            this.send('watch-transformers');
        }
        this.allTransformersHandlers.push(callback);
        return new changeSubscription(() => {
            this.allTransformersHandlers.remove(callback);
            if (this.allTransformersHandlers().length == 0) {
                this.send('unwatch-transformers');
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

    watchBulks(onChange: (e: bulkInsertChangeNotificationDto) => void) {
        var callback = new changesCallback<bulkInsertChangeNotificationDto>(onChange);
        if (this.allBulkInsertsHandlers().length == 0) {
            this.send('watch-bulk-operation');
        }
        this.allBulkInsertsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allBulkInsertsHandlers.remove(callback);
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
        this.disconnect();
        clearTimeout(this.pollQueueHandle);
    }

    private makeId() {
        var text = "";
        var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for (var i = 0; i < 5; i++)
            text += chars.charAt(Math.floor(Math.random() * chars.length));

        return text;
    }
}

export = changesApi;