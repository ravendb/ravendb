/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import resource = require("models/resources/resource");
import appUrl = require("common/appUrl");
import changeSubscription = require("common/changeSubscription");
import changesCallback = require("common/changesCallback");
import commandBase = require("commands/commandBase");
import folder = require("models/filesystem/folder");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import idGenerator = require("common/idGenerator");
import eventSourceSettingStorage = require("common/eventSourceSettingStorage");
import changesApiWarnStorage = require("common/changesApiWarnStorage");
import messagePublisher = require("common/messagePublisher");

class changesApi {

    private eventsId: string;
    private coolDownWithDataLoss: number;
    private isMultyTenantTransport:boolean;
    private resourcePath: string;
    public connectToChangesApiTask: JQueryDeferred<any>;
    private webSocket: WebSocket;
    static isServerSupportingWebSockets: boolean = true;
    private eventSource: EventSource;
    private readyStateOpen = 1;
    private isDisposing = false;

    private disposed: boolean = false;
    private isCleanClose: boolean = false;
    private normalClosureCode = 1000;
    private normalClosureMessage = "CLOSE_NORMAL";
    static messageWasShownOnce: boolean = false;
    private successfullyConnectedOnce: boolean = false;
    private sentMessages = [];
    private commandBase = new commandBase();

    private allReplicationConflicts = ko.observableArray<changesCallback<replicationConflictNotificationDto>>();
    private allDocsHandlers = ko.observableArray<changesCallback<documentChangeNotificationDto>>();
    private allIndexesHandlers = ko.observableArray<changesCallback<indexChangeNotificationDto>>();
    private allTransformersHandlers = ko.observableArray<changesCallback<transformerChangeNotificationDto>>();
    private watchedDocuments = {};
    private watchedPrefixes = {};
    private allBulkInsertsHandlers = ko.observableArray<changesCallback<bulkInsertChangeNotificationDto>>();

    private allFsSyncHandlers = ko.observableArray<changesCallback<synchronizationUpdateNotification>>();
    private allFsConflictsHandlers = ko.observableArray<changesCallback<synchronizationConflictNotification>>();
    private allFsConfigHandlers = ko.observableArray<changesCallback<filesystemConfigNotification>>();
    private allFsDestinationsHandlers = ko.observableArray<changesCallback<filesystemConfigNotification>>();
    private watchedFolders = {};

    private allCountersHandlers = ko.observableArray<changesCallback<counterChangeNotification>>();
    private watchedCounter = {};
    private watchedCountersInGroup = {};
    private allCounterBulkOperationsHandlers = ko.observableArray<changesCallback<counterBulkOperationNotificationDto>>();

    private allTimeSeriesHandlers = ko.observableArray<changesCallback<timeSeriesKeyChangeNotification>>();
    private watchedTimeSeries = {};
    private allTimeSeriesBulkOperationsHandlers = ko.observableArray<changesCallback<timeSeriesBulkOperationNotificationDto>>();
    
    constructor(private rs: resource, coolDownWithDataLoss: number = 0, isMultyTenantTransport:boolean = false) {
        this.eventsId = idGenerator.generateId();
        this.coolDownWithDataLoss = coolDownWithDataLoss;
        this.isMultyTenantTransport = isMultyTenantTransport;
        this.resourcePath = appUrl.forResourceQuery(this.rs);
        this.connectToChangesApiTask = $.Deferred();

        if ("WebSocket" in window && changesApi.isServerSupportingWebSockets) {
            this.connect(this.connectWebSocket);
        }
        else if ("EventSource" in window) {
            if (eventSourceSettingStorage.useEventSource()) {
                this.connect(this.connectEventSource);
            } else {
                this.connectToChangesApiTask.reject();
            }
        }
        else {
            //The browser doesn't support nor websocket nor eventsource
            //or we are in IE10 or IE11 and the server doesn't support WebSockets.
            //Anyway, at this point a warning message was already shown. 
            this.connectToChangesApiTask.reject();
        }
    }

    private connect(action: Function, recoveringFromWebsocketFailure: boolean = false) {
        if (this.disposed) {
            if (!!this.connectToChangesApiTask)
                this.connectToChangesApiTask.resolve();
            return;
        }
        if (!recoveringFromWebsocketFailure) {
            this.connectToChangesApiTask = $.Deferred();
        }
        var getTokenTask = new getSingleAuthTokenCommand(this.rs).execute();

        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                this.rs.isLoading(false);
                var token = tokenObject.Token;
                var connectionString = "singleUseAuthToken=" + token + "&id=" + this.eventsId + "&coolDownWithDataLoss=" + this.coolDownWithDataLoss + "&isMultyTenantTransport=" + this.isMultyTenantTransport;
                action.call(this, connectionString);
            })
            .fail((e) => {
                if (this.isDisposing) {
                    this.connectToChangesApiTask.reject();
                    return;
                }
                    
                var error = !!e.responseJSON ? e.responseJSON.Error : e.responseText;
                if (e.status === 0) {
                    // Connection has closed so try to reconnect every 3 seconds.
                    setTimeout(() => this.connect(action), 3 * 1000);
                }
                else if (e.status === ResponseCodes.ServiceUnavailable) {
                    // We're still loading the database, try to reconnect every 2 seconds.
                    if (this.rs.isLoading() === false) {
                        this.commandBase.reportError(error || "Failed to connect to changes", e.responseText, e.statusText);
                    }
                    this.rs.isLoading(true);
                    setTimeout(() => this.connect(action, true), 2 * 1000);
                }
                else if (e.status !== ResponseCodes.Forbidden) { // authorized connection
                    this.commandBase.reportError(error || "Failed to connect to changes", e.responseText, e.StatusText);
                    this.connectToChangesApiTask.reject();
                }
            });
    }

    private connectWebSocket(connectionString: string) {
        var connectionOpened: boolean = false;

        var wsProtocol = window.location.protocol === "https:" ? "wss://" : "ws://";
        var url = wsProtocol + window.location.host + this.resourcePath + "/changes/websocket?" + connectionString;
        this.webSocket = new WebSocket(url);

        this.webSocket.onmessage = (e) => this.onMessage(e);
        this.webSocket.onerror = (e) => {
            if (connectionOpened === false) {
                this.serverNotSupportingWebsocketsErrorHandler();
            } else {
                this.onError(e);
            }
        };
        this.webSocket.onclose = (e: CloseEvent) => {
            if (this.isCleanClose === false && changesApi.isServerSupportingWebSockets) {
                // Connection has closed uncleanly, so try to reconnect.
                this.connect(this.connectWebSocket);
            }
        }
        this.webSocket.onopen = () => {
            console.log("Connected to WebSocket changes API (" + this.rs.fullTypeName + " = " + this.rs.name + ")");
            this.reconnect();
            this.successfullyConnectedOnce = true;
            connectionOpened = true;
            this.connectToChangesApiTask.resolve();
        }
    }

    private connectEventSource(connectionString: string) {
        var connectionOpened: boolean = false;

        this.eventSource = new EventSource(this.resourcePath + "/changes/events?" + connectionString);

        this.eventSource.onmessage = (e) => this.onMessage(e);
        this.eventSource.onerror = (e) => {
            if (connectionOpened === false) {
                this.connectToChangesApiTask.reject();
            } else {
                this.onError(e);
                this.eventSource.close();
                this.connect(this.connectEventSource);
            }
        };
        this.eventSource.onopen = () => {
            console.log("Connected to EventSource changes API (" + this.rs.fullTypeName + " = " + this.rs.name + ")");
            this.reconnect();
            this.successfullyConnectedOnce = true;
            connectionOpened = true;
            this.connectToChangesApiTask.resolve();
        }
    }

    private reconnect() {
        if (this.successfullyConnectedOnce) {
            //send changes connection args after reconnecting
            this.sentMessages.forEach(args => this.send(args.command, args.value, false));
            
            ko.postbox.publish("ChangesApiReconnected", this.rs);

            if (changesApi.messageWasShownOnce) {
                this.commandBase.reportSuccess("Successfully reconnected to changes stream!");
                changesApi.messageWasShownOnce = false;
            }
        }
    }

    private onError(e: Event) {
        if (changesApi.messageWasShownOnce === false) {
            this.commandBase.reportError("Changes stream was disconnected!", "Retrying connection shortly.");
            changesApi.messageWasShownOnce = true;
        }
    }

    private serverNotSupportingWebsocketsErrorHandler() {
        var warningMessage: string;
        var details;

        if ("EventSource" in window) {
            if (eventSourceSettingStorage.useEventSource()) {
                this.connect(this.connectEventSource, true);
                warningMessage = "Your server doesn't support the WebSocket protocol!";
                details = "EventSource API is going to be used instead. However, multi tab usage isn't supported.\r\n" +
                "WebSockets are only supported on servers running on Windows Server 2012 and equivalent. \r\n" +
                " If you have issues with WebSockets on Windows Server 2012 and equivalent use Status > Debug > WebSocket to debug.";
                if (changesApi.isServerSupportingWebSockets) {
                    changesApi.isServerSupportingWebSockets = false;
                    if (changesApiWarnStorage.showChangesApiWarning()) {
                        this.commandBase.reportWarningWithButton(warningMessage, details, "Don't warn me again", () => {
                            if (changesApiWarnStorage.showChangesApiWarning()) {
                                changesApiWarnStorage.setValue(true);
                                messagePublisher.reportInfo("Disabled notification about WebSocket.");
                            }
                        });
                    }
                }
            } else {
                changesApi.isServerSupportingWebSockets = false;
                this.connectToChangesApiTask.reject();
            }
        } else {
            this.connectToChangesApiTask.reject();
            warningMessage = "Changes API is Disabled!";
            details = "Your server doesn't support the WebSocket protocol and your browser doesn't support the EventSource API.\r\n" +
            "In order to use it, please use a browser that supports the EventSource API.";
            if (changesApi.isServerSupportingWebSockets) {
                changesApi.isServerSupportingWebSockets = false;
                this.commandBase.reportWarning(warningMessage, details);
            }
        }
    }

    private send(command: string, value?: string, needToSaveSentMessages: boolean = true) {
        this.connectToChangesApiTask.done(() => {
            var args = {
                id: this.eventsId,
                command: command
            };
            if (value !== undefined) {
                args["value"] = value;
            }

            //TODO: exception handling?
            this.commandBase.query("/changes/config", args, this.rs)
                .done(() => this.saveSentMessages(needToSaveSentMessages, command, args));
        });
    }

    private saveSentMessages(needToSaveSentMessages: boolean, command: string, args) {
        if (needToSaveSentMessages) {
            if (command.slice(0, 2) === "un") {
                var commandName = command.slice(2, command.length);
                this.sentMessages = this.sentMessages.filter(msg => msg.command != commandName);
            } else {
                this.sentMessages.push(args);
            }
        }
    }

    private fireEvents<T>(events: Array<any>, param: T, filter: (T) => boolean) {
        for (var i = 0; i < events.length; i++) {
            if (filter(param)) {
                events[i].fire(param);
            }
        }
    }

    private onMessage(e: any) {
        var eventDto: changesApiEventDto = JSON.parse(e.data);
        var eventType = eventDto.Type;
        if (eventType === "Heartbeat") // ignore heartbeat
            return;

        var value = eventDto.Value;
        if (eventType === "DocumentChangeNotification") {
            this.fireEvents(this.allDocsHandlers(), value, (event) => true);

            for (var key in this.watchedDocuments) {
                var docCallbacks = <KnockoutObservableArray<documentChangeNotificationDto>> this.watchedDocuments[key];
                this.fireEvents(docCallbacks(), value, (event) => event.Id != null && event.Id === key);
            }

            for (var key in this.watchedPrefixes) {
                var docCallbacks = <KnockoutObservableArray<documentChangeNotificationDto>> this.watchedPrefixes[key];
                this.fireEvents(docCallbacks(), value, (event) => event.Id != null && event.Id.match("^" + key));
            }
        } else if (eventType === "IndexChangeNotification") {
            this.fireEvents(this.allIndexesHandlers(), value, (event) => true);
        } else if (eventType === "TransformerChangeNotification") {
            this.fireEvents(this.allTransformersHandlers(), value, (event) => true);
        } else if (eventType === "BulkInsertChangeNotification") {
            this.fireEvents(this.allBulkInsertsHandlers(), value, (event) => true);
        } else if (eventType === "SynchronizationUpdateNotification") {
            this.fireEvents(this.allFsSyncHandlers(), value, (event) => true);
        } else if (eventType === "ReplicationConflictNotification") {
            this.fireEvents(this.allReplicationConflicts(), value, (event) => true);
        } else if (eventType === "ConflictNotification") {
            this.fireEvents(this.allFsConflictsHandlers(), value, (event) => true);
        } else if (eventType === "FileChangeNotification") {
            for (var key in this.watchedFolders) {
                var folderCallbacks = <KnockoutObservableArray<fileChangeNotification>> this.watchedFolders[key];
                this.fireEvents(folderCallbacks(), value, (event) => {
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
                this.fireEvents(this.allFsDestinationsHandlers(), value, (e) => true);
            }
            this.fireEvents(this.allFsConfigHandlers(), value, (e) => true);
        } else if (eventType === "ChangeNotification") {
            this.fireEvents(this.allCountersHandlers(), value, (event) => true);
            //TODO: send events to other subscriptions
        } else if (eventType === "KeyChangeNotification") {
            this.fireEvents(this.allTimeSeriesHandlers(), value, (event) => true);
            //TODO: send events to other subscriptions
        } else {
            console.log("Unhandled Changes API notification type: " + eventType);
        }
    }

    watchAllIndexes(onChange: (e: indexChangeNotificationDto) => void) {
        var callback = new changesCallback<indexChangeNotificationDto>(onChange);
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

    watchAllTransformers(onChange: (e: transformerChangeNotificationDto) => void) {
        var callback = new changesCallback<transformerChangeNotificationDto>(onChange);
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
    }

    watchAllDocs(onChange: (e: documentChangeNotificationDto) => void) {
        var callback = new changesCallback<documentChangeNotificationDto>(onChange);
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

    watchDocument(docId: string, onChange: (e: documentChangeNotificationDto) => void): changeSubscription {
        var callback = new changesCallback<documentChangeNotificationDto>(onChange);
        if (typeof (this.watchedDocuments[docId]) === "undefined") {
            this.send("watch-doc", docId);
            this.watchedDocuments[docId] = ko.observableArray();
        }
        this.watchedDocuments[docId].push(callback);
        return new changeSubscription(() => {
            this.watchedDocuments[docId].remove(callback);
            if (this.watchedDocuments[docId]().length === 0) {
                delete this.watchedDocuments[docId];
                this.send("unwatch-doc", docId);
            }
        });
    }

    watchDocsStartingWith(docIdPrefix: string, onChange: (e: documentChangeNotificationDto) => void): changeSubscription {
        var callback = new changesCallback<documentChangeNotificationDto>(onChange);
        if (typeof (this.watchedPrefixes[docIdPrefix]) === "undefined") {
            this.send("watch-prefix", docIdPrefix);
            this.watchedPrefixes[docIdPrefix] = ko.observableArray();
        }
        this.watchedPrefixes[docIdPrefix].push(callback);
        return new changeSubscription(() => {
            this.watchedPrefixes[docIdPrefix].remove(callback);
            if (this.watchedPrefixes[docIdPrefix]().length === 0) {
                delete this.watchedPrefixes[docIdPrefix];
                this.send("unwatch-prefix", docIdPrefix);
            }
        });
    }

    watchBulks(onChange: (e: bulkInsertChangeNotificationDto) => void) {
        var callback = new changesCallback<bulkInsertChangeNotificationDto>(onChange);
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
    }

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
            this.watchedFolders[folder] = ko.observableArray();
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
            this.watchedCounter[counterId] = ko.observableArray();
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
            this.watchedCountersInGroup[group] = ko.observableArray();
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
            this.watchedTimeSeries[fullId] = ko.observableArray();
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
    }
    
    dispose() {
        this.isDisposing = true;
        this.disposed = true;
        this.connectToChangesApiTask.done(() => {
            var isCloseNeeded: boolean;

            if (this.webSocket && this.webSocket.readyState === this.readyStateOpen){
                console.log("Disconnecting from WebSocket changes API for (" + this.rs.fullTypeName + " = " + this.rs.name + ")");
                this.webSocket.close(this.normalClosureCode, this.normalClosureMessage);
                isCloseNeeded = true;
            }
            else if (this.eventSource && this.eventSource.readyState === this.readyStateOpen) {
                console.log("Disconnecting from EventSource changes API for (" + this.rs.fullTypeName + " = " + this.rs.name + ")");
                this.eventSource.close();
                isCloseNeeded = true;
            }

            if (isCloseNeeded) {
                this.send("disconnect", undefined, false);
                this.isCleanClose = true;
            }
        });
    }

    getResourceName() {
        return this.rs.name;
    }
}

export = changesApi;
