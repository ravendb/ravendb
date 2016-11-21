/// <reference path="../../typings/tsd.d.ts" />

import resource = require("models/resources/resource");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");

import abstractWebSocketClient = require("common/abstractWebSocketClient");

class changesApi extends abstractWebSocketClient {

    constructor(rs: resource) {
        super(rs);
    }

    serverStartTime = ko.observable<string>();

    //TODO: private allReplicationConflicts = ko.observableArray<changesCallback<replicationConflictNotificationDto>>();
    private allDocsHandlers = ko.observableArray<changesCallback<Raven.Abstractions.Data.DocumentChangeNotification>>();
    private allIndexesHandlers = ko.observableArray<changesCallback<Raven.Abstractions.Data.IndexChangeNotification>>();
    private allTransformersHandlers = ko.observableArray<changesCallback<Raven.Abstractions.Data.TransformerChangeNotification>>();
    //TODO: private allBulkInsertsHandlers = ko.observableArray<changesCallback<bulkInsertChangeNotificationDto>>();
    private allOperationsHandlers = ko.observableArray<changesCallback<Raven.Client.Data.OperationStatusChangeNotification>>();
    private allAlertsHandlers = ko.observableArray<changesCallback<Raven.Server.Alerts.AlertNotification>>();

    private watchedDocuments = new Map<string, KnockoutObservableArray<changesCallback<Raven.Abstractions.Data.DocumentChangeNotification>>>();
    private watchedPrefixes = new Map<string, KnockoutObservableArray<changesCallback<Raven.Abstractions.Data.DocumentChangeNotification>>>();
    private watchedOperations = new Map<number, KnockoutObservableArray<changesCallback<Raven.Client.Data.OperationStatusChangeNotification>>>();

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

    private onServerStartTimeReceived(startTime: string) {
        this.serverStartTime(startTime);
    }

    get connectionDescription() {
        return this.rs.fullTypeName + " = " + this.rs.name;
    }

    protected webSocketUrlFactory(token: singleAuthToken) {
        const connectionString = "singleUseAuthToken=" + token.Token + "&sendServerStartTime=true&throttleConnection=true";
        return "/changes?" + connectionString;
    }

    protected onError(e: any) {
        super.onError(e);
        this.serverStartTime(null);
    }

    protected onClose() {
        super.onClose();
        this.serverStartTime(null);
    }

    protected onMessage(e: any) {
        if (!e.data.trim()) {
            // it is heartbeat only
            return;
        }
        const eventDto: changesApiEventDto = JSON.parse(e.data);
        const eventType = eventDto.Type;
        const value = eventDto.Value;

        switch (eventType) {
            case "ServerStartTimeNotification":
                this.onServerStartTimeReceived(value as string);
                this.connectToWebSocketTask.resolve();
                ko.postbox.publish("ChangesApiReconnected", this.rs);
                break;
            case "DocumentChangeNotification":
                this.fireEvents<Raven.Abstractions.Data.DocumentChangeNotification>(this.allDocsHandlers(), value, () => true);

                this.watchedDocuments.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Abstractions.Data.DocumentChangeNotification>(callbacks(), value, (event) => event.Key != null && event.Key === key);
                });

                this.watchedPrefixes.forEach((callbacks, key) => {
                    this.fireEvents<Raven.Abstractions.Data.DocumentChangeNotification>(callbacks(), value, (event) => event.Key != null && event.Key.startsWith(key));
                });
                break;
            case "IndexChangeNotification":
                this.fireEvents<Raven.Abstractions.Data.IndexChangeNotification>(this.allIndexesHandlers(), value, () => true);
                break;
            case "TransformerChangeNotification":
                this.fireEvents<Raven.Abstractions.Data.TransformerChangeNotification>(this.allTransformersHandlers(), value, () => true);
                break;
            /* TODO: case "BulkInsertChangeNotification":
                this.fireEvents(this.allBulkInsertsHandlers(), value, () => true);
                break; */
            case "OperationStatusChangeNotification":
                this.fireEvents<Raven.Client.Data.OperationStatusChangeNotification>(this.allOperationsHandlers(), value, () => true);

                this.watchedOperations.forEach((callbacks, key) =>
                    this.fireEvents<Raven.Client.Data.OperationStatusChangeNotification>(callbacks(), value, (event) => event.OperationId === key));
                break;
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

    watchAllIndexes(onChange: (e: Raven.Abstractions.Data.IndexChangeNotification) => void) {
        var callback = new changesCallback<Raven.Abstractions.Data.IndexChangeNotification>(onChange);
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

    watchAllTransformers(onChange: (e: Raven.Abstractions.Data.TransformerChangeNotification) => void) {
        var callback = new changesCallback<Raven.Abstractions.Data.TransformerChangeNotification>(onChange);
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

    watchAllDocs(onChange: (e: Raven.Abstractions.Data.DocumentChangeNotification) => void) {
        var callback = new changesCallback<Raven.Abstractions.Data.DocumentChangeNotification>(onChange);

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

    watchDocument(docId: string, onChange: (e: Raven.Abstractions.Data.DocumentChangeNotification) => void): changeSubscription {
        let callback = new changesCallback<Raven.Abstractions.Data.DocumentChangeNotification>(onChange);

        if (!this.watchedDocuments.has(docId)) {
            this.send("watch-doc", docId);
            this.watchedDocuments.set(docId, ko.observableArray<changesCallback<Raven.Abstractions.Data.DocumentChangeNotification>>());
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

    watchDocsStartingWith(docIdPrefix: string, onChange: (e: Raven.Abstractions.Data.DocumentChangeNotification) => void): changeSubscription {
        let callback = new changesCallback<Raven.Abstractions.Data.DocumentChangeNotification>(onChange);

        if (!this.watchedPrefixes.has(docIdPrefix)) {
            this.send("watch-prefix", docIdPrefix);
            this.watchedPrefixes.set(docIdPrefix, ko.observableArray<changesCallback<Raven.Abstractions.Data.DocumentChangeNotification>>());
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

    watchOperation(operationId: number, onChange: (e: Raven.Client.Data.OperationStatusChangeNotification) => void): changeSubscription {
        let callback = new changesCallback<Raven.Client.Data.OperationStatusChangeNotification>(onChange);

        if (!this.watchedOperations.has(operationId)) {
            this.send("watch-operation", operationId.toString());
            this.watchedOperations.set(operationId, ko.observableArray<changesCallback<Raven.Client.Data.OperationStatusChangeNotification>>());
        }

        let callbacks = this.watchedOperations.get(operationId);
        callbacks.push(callback);

        return new changeSubscription(() => {
            callbacks.remove(callback);
            if (callbacks().length === 0) {
                this.watchedOperations.delete(operationId);
                this.send("unwatch-operation", operationId.toString());
            }
        });
    }

    watchOperations(onChange: (e: Raven.Client.Data.OperationStatusChangeNotification) => void): changeSubscription {
        const callback = new changesCallback<Raven.Client.Data.OperationStatusChangeNotification>(onChange);

        if (this.allOperationsHandlers().length === 0) {
            this.send("watch-operations");
        }

        this.allOperationsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allOperationsHandlers.remove(callback);
            if (this.allOperationsHandlers().length === 0) {
                this.send("unwatch-operations");
            }
        });
    }

    watchAlerts(onChange: (e: Raven.Server.Alerts.AlertNotification) => void): changeSubscription {
        const callback = new changesCallback<Raven.Server.Alerts.AlertNotification>(onChange);

        if (this.allAlertsHandlers().length === 0) {
            this.send("watch-alerts");
        }

        this.allAlertsHandlers.push(callback);

        return new changeSubscription(() => {
            this.allAlertsHandlers.remove(callback);
            if (this.allAlertsHandlers().length === 0) {
                this.send("unwatch-alerts");
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

