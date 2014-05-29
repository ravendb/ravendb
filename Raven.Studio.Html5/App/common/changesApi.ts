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
    private eventQueueName: string;
    private lastEventQueueCheckTime = new Date().getTime();
    private pollQueueHandle = 0;

    private static eventQueuePollInterval = 4000;
    private static eventQueueOwnerDeadTime = 10000;

    constructor(private db: database) {
        this.eventsId = this.makeId();
        this.eventQueueName = "Raven/Studio/ChangesApiEventQueue_" + db.name;
        this.connectOrReuseExistingConnection();
    }

    private connectOrReuseExistingConnection() {
        // If we already have the Studio opened in another tab, we must re-use that connection.
        // Why? Because some browsers, such as Chrome, limit the number of connections.
        // This is a problem if you have the Studio opened in multiple tabs, each creating its own connection.
        // When you reach the browser-enforced connection limit, all HTTP requests stop, thus breaking the Studio.
        //
        // To fix this, we share a single /changes connections between tabs using local storage.
        // All events go into a queue, that queue is stored in local storage.
        var eventQueue = this.loadEventQueue();
        if (this.isOwnerDead(eventQueue)) {
            this.takeOwnership(eventQueue);
            this.storeEventQueue(eventQueue);
        }

        // If we're not the owner of the event queue in local storage,
        // just consume that, rather than establish a new connection.
        if (this.isOwner(eventQueue)) {
            this.connect();
        }

        this.pollQueueHandle = setTimeout(() => this.monitorEventQueueOwnership(), changesApi.eventQueuePollInterval);
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

    private disconnect() {
        this.send('disconnect');
        if (this.source) {
            this.source.close();
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
        this.recordChangeInEventQueue(eventDto, e.data);
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
            } else {
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

    private loadEventQueue(): changesApiEventQueue {
        var queueJson: string = window.localStorage.getItem(this.eventQueueName);
        
        if (!queueJson) {
            return this.createAndStoreEventQueue();
        } else {
            try {
                return JSON.parse(queueJson);
            }
            catch (error) {
                return this.createAndStoreEventQueue();
            }
        }
    }

    private createAndStoreEventQueue(): changesApiEventQueue {
        var queue = this.createEventQueue();
        this.storeEventQueue(queue);
        return queue;
    }

    private createEventQueue(): changesApiEventQueue {
        return {
            ownerId: this.eventsId,
            name: this.db.name,
            lastHeartbeatMs: new Date().getTime(),
            events: []
        };
    }

    private storeEventQueue(queue: changesApiEventQueue) {
        window.localStorage.setItem(this.eventQueueName, JSON.stringify(queue));
    }

    private recordChangeInEventQueue(change: changesApiEventDto, changeDtoJson: string) {
        if (change.Type !== "Heartbeat") { // No need to record /changes API heartbeats.
            var queue = this.loadEventQueue();
            if (this.isOwner(queue)) {
                queue.events.push({
                    time: new Date().getTime(),
                    dtoJson: changeDtoJson
                });

                // For performance's sake, don't keep more than N events.
                // Events are typically processed within 5 seconds, and aren't used after that.
                var maxEvents = 50;
                if (queue.events.length > maxEvents) {
                    queue.events.splice(0, 1);
                }

                this.storeEventQueue(queue);
            }
        }
    }

    private isOwner(queue: changesApiEventQueue) {
        return queue.ownerId === this.eventsId;
    }

    private isOwnerDead(queue: changesApiEventQueue) {
        var differentOwner = queue.ownerId !== this.eventsId;
        var ownerIsDead = new Date().getTime() - queue.lastHeartbeatMs > changesApi.eventQueueOwnerDeadTime;
        return differentOwner && ownerIsDead;
    }

    private takeOwnership(queue: changesApiEventQueue) {
        queue.lastHeartbeatMs = new Date().getTime();
        queue.ownerId = this.eventsId;
    }

    private monitorEventQueueOwnership() {
        var queue = this.loadEventQueue();
        var nowMs = new Date().getTime();
        var isExpectedToBeOwner = !!this.source;
        var isOwner = this.isOwner(queue);
        if (this.isOwnerDead(queue)) {
            // Owner is dead! Taking ownership.
            this.connect();
            this.takeOwnership(queue);
            this.storeEventQueue(queue);
        }
        else if (isExpectedToBeOwner && isOwner) {
            // We're the owner. Record a heartbeat and move on.
            queue.lastHeartbeatMs = new Date().getTime();
            this.storeEventQueue(queue);
        } else if (isExpectedToBeOwner && !isOwner) {
            // Somebody else grabbed our queue and thought we were dead.
            // Rumors of my demise are greatly exaggerated!
            // This should never happen. But, no worries, we'll graciously let them hold the connection.
            this.disconnect();
        } else if (!isExpectedToBeOwner && !isOwner) {
            // Someone else is the owner. Consume the events in the shared queue.
            queue.events
                .filter(e => e.time > this.lastEventQueueCheckTime)
                .reverse() // So that the oldest ones are processed first.
                .map(e => <changesApiEventDto>JSON.parse(e.dtoJson))
                .forEach(e => this.processEvent(e));
            this.lastEventQueueCheckTime = nowMs;
        }

        this.pollQueueHandle = setTimeout(() => this.monitorEventQueueOwnership(), changesApi.eventQueuePollInterval);
    }
}

export = changesApi;