/// <reference path="../../typings/tsd.d.ts" />

import resource = require("models/resources/resource");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");

import EVENTS = require("common/constants/events");

import eventsWebSocketClient = require("common/eventsWebSocketClient");

class changesApi extends eventsWebSocketClient<changesApiEventDto> {

    constructor(rs: resource) {
        super(rs);
    }

    //TODO: private allReplicationConflicts = ko.observableArray<changesCallback<replicationConflictNotificationDto>>();
    private allDocsHandlers = ko.observableArray<changesCallback<Raven.Client.Data.DocumentChange>>();
    private allIndexesHandlers = ko.observableArray<changesCallback<Raven.Client.Data.IndexChange>>();
    private allTransformersHandlers = ko.observableArray<changesCallback<Raven.Client.Data.TransformerChange>>();
    //TODO: private allBulkInsertsHandlers = ko.observableArray<changesCallback<bulkInsertChangeNotificationDto>>();

    private watchedDocuments = new Map<string, KnockoutObservableArray<changesCallback<Raven.Client.Data.DocumentChange>>>();
    private watchedPrefixes = new Map<string, KnockoutObservableArray<changesCallback<Raven.Client.Data.DocumentChange>>>();
    private watchedIndexes = new Map<string, KnockoutObservableArray<changesCallback<Raven.Client.Data.IndexChange>>>();

    /* TODO:
    private allFsSyncHandlers = ko.observableArray<changesCallback<synchronizationUpdateNotification>>();
    private allFsConflictsHandlers = ko.observableArray<changesCallback<synchronizationConflictNotification>>();
    private allFsConfigHandlers = ko.observableArray<changesCallback<filesystemConfigNotification>>();
    private allFsDestinationsHandlers = ko.observableArray<changesCallback<filesystemConfigNotification>>();
    private watchedFolders: dictionary<KnockoutObservableArray<changesCallback<fileChangeNotification>>> = {};

    private allCountersHandlers = ko.observableArray<changesCallback<counterChangeNotification>>();
    private watchedCounter: dictionary<KnockoutObservableArray<changesCallback<counterChangeNotification>>> = {};
    private watchedCountersInGroup: dictionary<KnockoutObservableArray<changesCallback<countersInGroupNotification>>> = {};
    private allCounterBulkOperationsHandlers = ko.observableArray<changesCallback<counterBulkOperationNotificationDto>>();

    private allTimeSeriesHandlers = ko.observableArray<changesCallback<timeSeriesKeyChangeNotification>>();
    private watchedTimeSeries: dictionary<KnockoutObservableArray<changesCallback<timeSeriesKeyChangeNotification>>> = {};
    private allTimeSeriesBulkOperationsHandlers = ko.observableArray<changesCallback<timeSeriesBulkOperationNotificationDto>>();*/

    get connectionDescription() {
        return this.rs.fullTypeName + " = " + this.rs.name;
    }

    protected webSocketUrlFactory(token: singleAuthToken) {
        const connectionString = "singleUseAuthToken=" + token.Token;
        return "/changes?" + connectionString;
    }

    protected onOpen() {
        super.onOpen();
        ko.postbox.publish(EVENTS.ChangesApi.Reconnected, this.rs);
    }

    protected onMessage(eventDto: changesApiEventDto) {
        const eventType = eventDto.Type;
        const value = eventDto.Value;

        switch (eventType) {
            case "DocumentChangeNotification":
                this.fireEvents<Raven.Client.Data.DocumentChange>(this.allDocsHandlers(), value, () => true);

                this.watchedDocuments.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Client.Data.DocumentChange>(callbacks(), value, (event) => event.Key != null && event.Key === key);
                });

                this.watchedPrefixes.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Client.Data.DocumentChange>(callbacks(), value, (event) => event.Key != null && event.Key.startsWith(key));
                });
                break;
            case "IndexChangeNotification":
                this.fireEvents<Raven.Client.Data.IndexChange>(this.allIndexesHandlers(), value, () => true);

                this.watchedIndexes.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Client.Data.IndexChange>(callbacks(), value, (event) => event.Name != null && event.Name === key);
                });
                break;
            case "TransformerChangeNotification":
                this.fireEvents<Raven.Client.Data.TransformerChange>(this.allTransformersHandlers(), value, () => true);
                break;
            /* TODO: case "BulkInsertChangeNotification":
                this.fireEvents(this.allBulkInsertsHandlers(), value, () => true);
                break; */
            default:
                console.log("Unhandled Changes API notification type: " + eventType);
        }

        /* TODO:} else if (eventType === "SynchronizationUpdateNotification") {
            this.fireEvents<typeHere>(this.allFsSyncHandlers(), value, () => true);
        } else if (eventType === "ReplicationConflictNotification") {
            this.fireEvents<typeHere>(this.allReplicationConflicts(), value, () => true);
        } else if (eventType === "ConflictNotification") {
            this.fireEvents<typeHere>(this.allFsConflictsHandlers(), value, () => true);
        } else if (eventType === "FileChangeNotification") {
            for (var key in this.watchedFolders) {
                var folderCallbacks = this.watchedFolders[key];
                this.fireEvents<typeHere>(folderCallbacks(), value, (event) => {
                    var notifiedFolder = folder.getFolderFromFilePath(event.File);
                    var match: string[] = null;
                    if (notifiedFolder && notifiedFolder.path) {
                        match = notifiedFolder.path.match(key);
                    }
                    return match && match.length > 0;
                });
            }
        } else if (eventType === "ConfigurationChangeNotification") {
            if (value.Name.indexOf("Raven/Synchronization/Destinations") >= 0) {
                this.fireEvents<typeHere>(this.allFsDestinationsHandlers(), value, () => true);
            }
            this.fireEvents<typeHere>(this.allFsConfigHandlers(), value, () => true);
        } else if (eventType === "ChangeNotification") {
            this.fireEvents<typeHere>(this.allCountersHandlers(), value, () => true);
            //TODO: send events to other subscriptions
        } else if (eventType === "KeyChangeNotification") {
            this.fireEvents<typeHere>(this.allTimeSeriesHandlers(), value, () => true);
            //TODO: send events to other subscriptions*/
    }

    watchAllIndexes(onChange: (e: Raven.Client.Data.IndexChange) => void) {
        var callback = new changesCallback<Raven.Client.Data.IndexChange>(onChange);
        if (this.allIndexesHandlers().length === 0) {
            this.send("watch-indexes");
        }
        this.allIndexesHandlers.push(callback);
        return new changeSubscription(() => {
            this.allIndexesHandlers.remove(callback);
            if (this.allIndexesHandlers().length === 0) {
                this.send("unwatch-indexes");
            }
        });
    }

    watchIndex(indexName: string, onChange: (e: Raven.Client.Data.IndexChange) => void): changeSubscription {
        let callback = new changesCallback<Raven.Client.Data.IndexChange>(onChange);

        if (!this.watchedIndexes.has(indexName)) {
            this.send("watch-index", indexName);
            this.watchedIndexes.set(indexName, ko.observableArray<changesCallback<Raven.Client.Data.IndexChange>>());
        }

        let callbacks = this.watchedIndexes.get(indexName);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedIndexes.delete(indexName);
                this.send("unwatch-index", indexName);
            }
        });
    }

    watchAllTransformers(onChange: (e: Raven.Client.Data.TransformerChange) => void) {
        var callback = new changesCallback<Raven.Client.Data.TransformerChange>(onChange);
        if (this.allTransformersHandlers().length === 0) {
            this.send("watch-transformers");
        }
        this.allTransformersHandlers.push(callback);
        return new changeSubscription(() => {
            this.allTransformersHandlers.remove(callback);
            if (this.allTransformersHandlers().length === 0) {
                this.send("unwatch-transformers");
            }
        });
    }

    /*TODO: 
    watchAllReplicationConflicts(onChange: (e: replicationConflictNotificationDto) => void) {
        var callback = new changesCallback<replicationConflictNotificationDto>(onChange);
        if (this.allReplicationConflicts().length === 0) {
            this.send("watch-replication-conflicts");
        }
        this.allReplicationConflicts.push(callback);
        return new changeSubscription(() => {
            this.allReplicationConflicts.remove(callback);
            if (this.allReplicationConflicts().length === 0) {
                this.send("unwatch-replication-conflicts");
            }
        });
    }*/

    watchAllDocs(onChange: (e: Raven.Client.Data.DocumentChange) => void) {
        var callback = new changesCallback<Raven.Client.Data.DocumentChange>(onChange);

        if (this.allDocsHandlers().length === 0) {
            this.send("watch-docs");
        }

        this.allDocsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allDocsHandlers.remove(callback);
            if (this.allDocsHandlers().length === 0) {
                this.send("unwatch-docs");
            }
        });
    }

    watchDocument(docId: string, onChange: (e: Raven.Client.Data.DocumentChange) => void): changeSubscription {
        let callback = new changesCallback<Raven.Client.Data.DocumentChange>(onChange);

        if (!this.watchedDocuments.has(docId)) {
            this.send("watch-doc", docId);
            this.watchedDocuments.set(docId, ko.observableArray<changesCallback<Raven.Client.Data.DocumentChange>>());
        }

        let callbacks = this.watchedDocuments.get(docId);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedDocuments.delete(docId);
                this.send("unwatch-doc", docId);
            }
        });
    }

    watchDocsStartingWith(docIdPrefix: string, onChange: (e: Raven.Client.Data.DocumentChange) => void): changeSubscription {
        let callback = new changesCallback<Raven.Client.Data.DocumentChange>(onChange);

        if (!this.watchedPrefixes.has(docIdPrefix)) {
            this.send("watch-prefix", docIdPrefix);
            this.watchedPrefixes.set(docIdPrefix, ko.observableArray<changesCallback<Raven.Client.Data.DocumentChange>>());
        }

        let callbacks = this.watchedPrefixes.get(docIdPrefix);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedPrefixes.delete(docIdPrefix);
                this.send("unwatch-prefix", docIdPrefix);
            }
        });
    }

    /* TODO
    watchBulks(onChange: (e: bulkInsertChangeNotificationDto) => void) {
        let callback = new changesCallback<bulkInsertChangeNotificationDto>(onChange);

        if (this.allBulkInsertsHandlers().length === 0) {
            this.send("watch-bulk-operation");
        }

        this.allBulkInsertsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allBulkInsertsHandlers.remove(callback);
            if (this.allDocsHandlers().length === 0) {
                this.send('unwatch-bulk-operation');
            }
        });
    }*/

    /* TODO:
    watchFsSync(onChange: (e: synchronizationUpdateNotification) => void): changeSubscription {
        var callback = new changesCallback<synchronizationUpdateNotification>(onChange);
        if (this.allFsSyncHandlers().length === 0) {
            this.send("watch-sync");
        }
        this.allFsSyncHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsSyncHandlers.remove(callback);
            if (this.allFsSyncHandlers().length === 0) {
                this.send("unwatch-sync");
            }
        });
    }

    watchFsConflicts(onChange: (e: synchronizationConflictNotification) => void) : changeSubscription {
        var callback = new changesCallback<synchronizationConflictNotification>(onChange);
        if (this.allFsConflictsHandlers().length === 0) {
            this.send("watch-conflicts");
        }
        this.allFsConflictsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsConflictsHandlers.remove(callback);
            if (this.allFsConflictsHandlers().length === 0) {
                this.send("unwatch-conflicts");
            }
        });
    }

    watchFsFolders(folder: string, onChange: (e: fileChangeNotification) => void): changeSubscription {
        var callback = new changesCallback<fileChangeNotification>(onChange);
        if (typeof (this.watchedFolders[folder]) === "undefined") {
            this.send("watch-folder", folder);
            this.watchedFolders[folder] = ko.observableArray<changesCallback<fileChangeNotification>>();
        }
        this.watchedFolders[folder].push(callback);
        return new changeSubscription(() => {
            this.watchedFolders[folder].remove(callback);
            if (this.watchedFolders[folder].length === 0) {
                delete this.watchedFolders[folder];
                this.send("unwatch-folder", folder);
            }
        });
    }

    watchFsConfig(onChange: (e: filesystemConfigNotification) => void): changeSubscription {
        var callback = new changesCallback<filesystemConfigNotification>(onChange);
        if (this.allFsConfigHandlers().length === 0) {
            this.send("watch-config");
        }
        this.allFsConfigHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsConfigHandlers.remove(callback);
            if (this.allFsConfigHandlers().length === 0) {
                this.send("unwatch-config");
            }
        });
    }

    watchFsDestinations(onChange: (e: filesystemConfigNotification) => void): changeSubscription {
        var callback = new changesCallback<filesystemConfigNotification>(onChange);
        if (this.allFsDestinationsHandlers().length === 0) {
            this.send("watch-config");
        }
        this.allFsDestinationsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsDestinationsHandlers.remove(callback);
            if (this.allFsDestinationsHandlers().length === 0) {
                this.send("unwatch-config");
            }
        });
    }

    watchAllCounters(onChange: (e: counterChangeNotification) => void) {
        var callback = new changesCallback<counterChangeNotification>(onChange);
        if (this.allDocsHandlers().length === 0) {
            this.send("watch-counters");
        }
        this.allCountersHandlers.push(callback);
        return new changeSubscription(() => {
            this.allCountersHandlers.remove(callback);
            if (this.allDocsHandlers().length === 0) {
                this.send("unwatch-counters");
            }
        });
    }

    watchCounterChange(groupName: string, counterName: string, onChange: (e: counterChangeNotification) => void): changeSubscription {
        var counterId = groupName + "/" + counterName;
        var callback = new changesCallback<counterChangeNotification>(onChange);
        if (typeof (this.watchedCounter[counterId]) === "undefined") {
            this.send("watch-counter-change", counterId);
            this.watchedCounter[counterId] = ko.observableArray<changesCallback<counterChangeNotification>>();
        }
        this.watchedCounter[counterId].push(callback);
        return new changeSubscription(() => {
            this.watchedCounter[counterId].remove(callback);
            if (this.watchedCounter[counterId]().length === 0) {
                delete this.watchedCounter[counterId];
                this.send("unwatch-counter-change", counterId);
            }
        });
    }

    watchCountersInGroup(group: string, onChange: (e: countersInGroupNotification) => void): changeSubscription {
        var callback = new changesCallback<countersInGroupNotification>(onChange);
        if (typeof (this.watchedCountersInGroup[group]) === "undefined") {
            this.send("watch-counters-in-group", group);
            this.watchedCountersInGroup[group] = ko.observableArray<changesCallback<countersInGroupNotification>>();
        }
        this.watchedCountersInGroup[group].push(callback);
        return new changeSubscription(() => {
            this.watchedCountersInGroup[group].remove(callback);
            if (this.watchedCountersInGroup[group]().length === 0) {
                delete this.watchedCountersInGroup[group];
                this.send("unwatch-counters-in-group", group);
            }
        });
    }

    watchCounterBulkOperation(onChange: (e: counterBulkOperationNotificationDto) => void) {
        var callback = new changesCallback<counterBulkOperationNotificationDto>(onChange);
        if (this.allCounterBulkOperationsHandlers().length === 0) {
            this.send("watch-bulk-operation");
        }
        this.allCounterBulkOperationsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allCounterBulkOperationsHandlers.remove(callback);
            if (this.allDocsHandlers().length === 0) {
                this.send("unwatch-bulk-operation");
            }
        });
    }

    watchTimeSeriesChange(type: string, key: string, onChange: (e: timeSeriesKeyChangeNotification) => void): changeSubscription {
        var fullId = type + "/" + key;
        var callback = new changesCallback<timeSeriesKeyChangeNotification>(onChange);
        if (typeof (this.watchedTimeSeries[fullId]) === "undefined") {
            this.send("watch-time-series-key-change", fullId);
            this.watchedTimeSeries[fullId] = ko.observableArray<changesCallback<timeSeriesKeyChangeNotification>>();
        }
        this.watchedTimeSeries[fullId].push(callback);
        return new changeSubscription(() => {
            this.watchedTimeSeries[fullId].remove(callback);
            if (this.watchedTimeSeries[fullId]().length === 0) {
                delete this.watchedTimeSeries[fullId];
                this.send("unwatch-time-series-key-change", fullId);
            }
        });
    }

    watchAllTimeSeries(onChange: (e: timeSeriesKeyChangeNotification) => void) {
        var callback = new changesCallback<timeSeriesKeyChangeNotification>(onChange);
        if (this.allDocsHandlers().length === 0) {
            this.send("watch-time-series");
        }
        this.allTimeSeriesHandlers.push(callback);
        return new changeSubscription(() => {
            this.allTimeSeriesHandlers.remove(callback);
            if (this.allDocsHandlers().length === 0) {
                this.send("unwatch-time-series");
            }
        });
    }
    
    watchTimeSeriesBulkOperation(onChange: (e: timeSeriesBulkOperationNotificationDto) => void) {
        var callback = new changesCallback<timeSeriesBulkOperationNotificationDto>(onChange);
        if (this.allTimeSeriesBulkOperationsHandlers().length === 0) {
            this.send("watch-bulk-operation");
        }
        this.allTimeSeriesBulkOperationsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allTimeSeriesBulkOperationsHandlers.remove(callback);
            if (this.allDocsHandlers().length === 0) {
                this.send("unwatch-bulk-operation");
            }
        });
    }*/
   
}

export = changesApi;

