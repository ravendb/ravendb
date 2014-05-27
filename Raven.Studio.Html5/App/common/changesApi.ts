/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import database = require('models/database');
import appUrl = require('common/appUrl');
import changeSubscription = require('models/changeSubscription');
import changesCallback = require('common/changesCallback');
import commandBase = require('commands/commandBase');

class changesApi {

    private eventsId: string;
    private source: EventSource;

    private allDocsHandlers = ko.observableArray<changesCallback<documentChangeNotificationDto>>();
    private allIndexesHandlers = ko.observableArray<changesCallback<indexChangeNotificationDto>>();
    private allTransformersHandlers = ko.observableArray<changesCallback<transformerChangeNotificationDto>>();
    private watchedPrefixes = {};
    private allBulkInsertsHandlers = ko.observableArray<changesCallback<bulkInsertChangeNotificationDto>>();
    private commandBase = new commandBase();
    private sharedConnection = this.loadSharedConnection();
    private sharedConnectionLastPingMs = new Date().getTime();
    private sharedConnectionLiveHeartbeatHandle: number;
    
    private static sharedConnectionPingInterval = 5000;
    private static sharedConnectionName = "Raven/Studio/ChangesApiConnection";

    constructor(private db: database) {
        this.eventsId = this.makeId();
        this.connectOrReuseExistingConnection();
    }

    private connectOrReuseExistingConnection() {
        // If we already have the Studio opened in another tab, we must re-use that connection.
        // Why? Because some browsers, such as Chrome, limit the number of connections.
        // When this happens, Chrome suspends all HTTP requests, which stops the Studio from working.
        //
        // To fix this, we "share" a connection with the first opened tab.
        // How? By using local storage, shared between tabs on the same host, to notify of events.
        // Only the first tab has a real connection, while the other tabs will get notifications via local storage polling.
        var connectionForDb = this.getSharedConnectionForDb();
        if (this.isConnectionOwnedByOtherTab(connectionForDb) && this.isConnectionLive(connectionForDb)) {
            this.reuseExistingConnection();
        } else {
            this.createNewConnection();
        }
    }

    private createNewConnection() {
        this.connect();
        this.recordHeartbeatInSharedConnection();
    }

    private recordHeartbeatInSharedConnection() {
        var connection = this.getSharedConnectionForDb();
        connection.lastHeartbeatMs = new Date().getTime();
        this.storeSharedConnection();
        this.sharedConnectionLiveHeartbeatHandle = setTimeout(() => this.recordHeartbeatInSharedConnection(), 3000);
    }

    private connect() {
        if (!!window.EventSource) {
            var dbUrl = appUrl.forResourceQuery(this.db);

            console.log("Connecting to changes API (db = " + this.db.name + ")");

            this.source = new EventSource(dbUrl + '/changes/events?id=' + this.eventsId);
            this.source.onmessage = (e) => this.onMessage(e);
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

    private onMessage(e: any) {
        this.recordChangeOnSharedConnection(e.data);
        this.processEvent(e.data);
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

    private processEvent(eventJson: string) {
        var json = JSON.parse(eventJson);
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
        } else if (type === "TransformerChangeNotification") {
            this.fireEvents(this.allTransformersHandlers(), json.Value, (e) => true);
        } else {
            console.log("Unhandled Changes API notification type: " + type);
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
            clearTimeout(this.sharedConnectionLiveHeartbeatHandle);
        }
    }

    private makeId() {
        var text = "";
        var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        for (var i = 0; i < 5; i++)
            text += chars.charAt(Math.floor(Math.random() * chars.length));

        return text;
    }

    private loadSharedConnection(): sharedChangesConnection {
        var connectionJson: string = window.localStorage.getItem(changesApi.sharedConnectionName);

        if (!connectionJson) {
            return this.createAndStoreNewConnection();
        } else {
            try {
                return JSON.parse(connectionJson);
            }
            catch (error) {
                this.createAndStoreNewConnection();
            }
        }
    }

    private createAndStoreNewConnection(): sharedChangesConnection {
        var connection: sharedChangesConnection = {
            databases: [this.createSharedConnectionForDatabase()]
        };
        localStorage.setItem(changesApi.sharedConnectionName, JSON.stringify(connection));
        return connection;
    }

    private createSharedConnectionForDatabase(): sharedChangesConnectionDatabase {
        return {
            id: this.eventsId,
            name: this.db.name,
            lastHeartbeatMs: new Date().getTime(),
            events: []
        };
    }

    private getSharedConnectionForDb(): sharedChangesConnectionDatabase {
        var sharedConnectionForDb = this.sharedConnection.databases.first(db => db.name === this.db.name);
        if (sharedConnectionForDb) {
            return sharedConnectionForDb;
        } else {
            sharedConnectionForDb = this.createSharedConnectionForDatabase();
            this.sharedConnection.databases.push(sharedConnectionForDb);
            this.storeSharedConnection();
            return sharedConnectionForDb;
        }
    }

    private storeSharedConnection() {
        window.localStorage.setItem(changesApi.sharedConnectionName, JSON.stringify(this.sharedConnection));
    }

    private reuseExistingConnection() {
        setTimeout(() => this.pingSharedConnectionForUpdates(), changesApi.sharedConnectionPingInterval);
    }

    private pingSharedConnectionForUpdates() {
        this.sharedConnection = this.loadSharedConnection();
        var connectionForDb = this.getSharedConnectionForDb();
        if (this.isConnectionOwnedByOtherTab(connectionForDb) && this.isConnectionLive(connectionForDb)) {
            try {
                this.processSharedConnectionEvents(connectionForDb);
            } finally {
                setTimeout(() => this.pingSharedConnectionForUpdates(), changesApi.sharedConnectionPingInterval);
            }
        } else {
            // The other tab has been closed or disconnected.
            this.takeoverAsPrimaryConnection();
        }
    }

    private takeoverAsPrimaryConnection() {
        var connection = this.getSharedConnectionForDb();
        if (connection.id !== this.eventsId) {
            connection.id = this.eventsId;
            connection.lastHeartbeatMs = new Date().getTime();
            this.storeSharedConnection();
            this.connect();
        }
    }

    private processSharedConnectionEvents(connection: sharedChangesConnectionDatabase) {
        var eventsAfterLastPing = connection
            .events
            .filter(e => e.time >= this.sharedConnectionLastPingMs);
        try {
            eventsAfterLastPing.forEach(e => this.processEvent(e.eventJson));
        } finally {
            this.sharedConnectionLastPingMs = new Date().getTime();
        }
    }

    private isConnectionOwnedByOtherTab(connection: sharedChangesConnectionDatabase) {
        return connection.id !== this.eventsId;
    }

    private isConnectionLive(connection: sharedChangesConnectionDatabase) {
        var consideredAliveMs = 10000;
        var timeSinceLastHeartbeat = new Date().getTime() - connection.lastHeartbeatMs;
        return timeSinceLastHeartbeat < consideredAliveMs;
    }

    private recordChangeOnSharedConnection(eventJson: string) {
        var sharedConnectionForDb = this.getSharedConnectionForDb();
        sharedConnectionForDb.events.push({
            time: new Date().getTime(),
            eventJson: eventJson
        });

        // For performance's sake, don't keep more than 100 events.
        if (sharedConnectionForDb.events.length > 100) {
            sharedConnectionForDb.events.splice(0, 1);
        }

        this.storeSharedConnection();
    }
}

export = changesApi;