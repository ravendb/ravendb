/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import resource = require('models/resources/resource');
import appUrl = require('common/appUrl');
import changeSubscription = require('common/changeSubscription');
import changesCallback = require('common/changesCallback');
import commandBase = require('commands/commandBase');
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

    private isCleanClose: boolean = false;
    private normalClosureCode = 1000;
    private normalClosureMessage = "CLOSE_NORMAL";
    static messageWasShownOnce: boolean = false;
    private successfullyConnectedOnce: boolean = false;
    private sentMessages = [];

    private allReplicationConflicts = ko.observableArray<changesCallback<replicationConflictNotificationDto>>();
    private allDocsHandlers = ko.observableArray<changesCallback<documentChangeNotificationDto>>();
    private allIndexesHandlers = ko.observableArray<changesCallback<indexChangeNotificationDto>>();
    private allTransformersHandlers = ko.observableArray<changesCallback<transformerChangeNotificationDto>>();
    private watchedPrefixes = {};
    private allBulkInsertsHandlers = ko.observableArray<changesCallback<bulkInsertChangeNotificationDto>>();
    private allFsSyncHandlers = ko.observableArray<changesCallback<synchronizationUpdateNotification>>();
    private allFsConflictsHandlers = ko.observableArray<changesCallback<synchronizationConflictNotification>>();
    private allFsConfigHandlers = ko.observableArray<changesCallback<filesystemConfigNotification>>();
    private allFsDestinationsHandlers = ko.observableArray<changesCallback<filesystemConfigNotification>>();
    private watchedFolders = {};
    private commandBase = new commandBase();
    
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
        if (!recoveringFromWebsocketFailure) {
            this.connectToChangesApiTask = $.Deferred();
        }
        var getTokenTask = new getSingleAuthTokenCommand(this.rs).execute();

        getTokenTask
            .done((tokenObject: singleAuthToken) => {
                this.rs.isLoading(false);
                var token = tokenObject.Token;
                var connectionString = 'singleUseAuthToken=' + token + '&id=' + this.eventsId + '&coolDownWithDataLoss=' + this.coolDownWithDataLoss +  '&isMultyTenantTransport=' +this.isMultyTenantTransport;
                action.call(this, connectionString);
            })
            .fail((e) => {
                var error = !!e.responseJSON ? e.responseJSON.Error : e.responseText;
                if (e.status == 0) {
                    // Connection has closed so try to reconnect every 3 seconds.
                    setTimeout(() => this.connect(action), 3 * 1000);
                }
                else if (e.status == ResponseCodes.ServiceUnavailable) {
                    // We're still loading the database, try to reconnect every 2 seconds.
                    if (this.rs.isLoading() == false) {
                        this.commandBase.reportError(error);
                    }
                    this.rs.isLoading(true);
                    setTimeout(() => this.connect(action, true), 2 * 1000);
                }
                else if (e.status != ResponseCodes.Forbidden) { // authorized connection
                    this.commandBase.reportError(error);
                    this.connectToChangesApiTask.reject();
                }
            });
    }

    private connectWebSocket(connectionString: string) {
        var connectionOpened: boolean = false;

        var wsProtocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
	    var url = wsProtocol + window.location.host + this.resourcePath + '/changes/websocket?' + connectionString;
	    this.webSocket = new WebSocket(url);

        this.webSocket.onmessage = (e) => this.onMessage(e);
        this.webSocket.onerror = (e) => {
            if (connectionOpened == false) {
                this.serverNotSupportingWebsocketsErrorHandler();
            } else {
                this.onError(e);
            }
        };
        this.webSocket.onclose = (e: CloseEvent) => {
            if (this.isCleanClose == false && changesApi.isServerSupportingWebSockets) {
                // Connection has closed uncleanly, so try to reconnect.
                this.connect(this.connectWebSocket);
            }
        }
        this.webSocket.onopen = () => {
            console.log("Connected to WebSocket changes API (rs = " + this.rs.name + ")");
            this.reconnect();
            this.successfullyConnectedOnce = true;
            connectionOpened = true;
            this.connectToChangesApiTask.resolve();
        }
    }

    private connectEventSource(connectionString: string) {
        var connectionOpened: boolean = false;

        this.eventSource = new EventSource(this.resourcePath + '/changes/events?' + connectionString);

        this.eventSource.onmessage = (e) => this.onMessage(e);
        this.eventSource.onerror = (e) => {
            if (connectionOpened == false) {
                this.connectToChangesApiTask.reject();
            } else {
                this.onError(e);
                this.eventSource.close();
                this.connect(this.connectEventSource);
            }
        };
        this.eventSource.onopen = () => {
            console.log("Connected to EventSource changes API (rs = " + this.rs.name + ")");
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
        if (changesApi.messageWasShownOnce == false) {
            this.commandBase.reportError("Changes stream was disconnected!", "Retrying connection shortly.");
            changesApi.messageWasShownOnce = true;
        }
    }

    private serverNotSupportingWebsocketsErrorHandler() {
        var warningMessage;
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
            this.commandBase.query('/changes/config', args, this.rs)
                .done(() => this.saveSentMessages(needToSaveSentMessages, command, args));
        });
    }

    private saveSentMessages(needToSaveSentMessages: boolean, command: string, args) {
        if (needToSaveSentMessages) {
            if (command.slice(0, 2) == "un") {
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
        var type = eventDto.Type;
        var value = eventDto.Value;

        if (type !== "Heartbeat") { // ignore heartbeat
            if (type === "DocumentChangeNotification") {
                this.fireEvents(this.allDocsHandlers(), value, (e) => true);
                for (var key in this.watchedPrefixes) {
                    var docCallbacks = <KnockoutObservableArray<documentChangeNotificationDto>> this.watchedPrefixes[key];
                    this.fireEvents(docCallbacks(), value, (e) => e.Id != null && e.Id.match("^" + key));
                }
            } else if (type === "IndexChangeNotification") {
                this.fireEvents(this.allIndexesHandlers(), value, (e) => true);
            } else if (type === "TransformerChangeNotification") {
                this.fireEvents(this.allTransformersHandlers(), value, (e) => true);
            } else if (type === "BulkInsertChangeNotification") {
                this.fireEvents(this.allBulkInsertsHandlers(), value, (e) => true);
            } else if (type === "SynchronizationUpdateNotification") {
                this.fireEvents(this.allFsSyncHandlers(), value, (e) => true);
            } else if (type === "ReplicationConflictNotification") {
                this.fireEvents(this.allReplicationConflicts(), value, (e) => true);
            } else if (type === "ConflictNotification") {
                this.fireEvents(this.allFsConflictsHandlers(), value, (e) => true);
            } else if (type === "FileChangeNotification") {
                for (var key in this.watchedFolders) {
                    var folderCallbacks = <KnockoutObservableArray<fileChangeNotification>> this.watchedFolders[key];
                    this.fireEvents(folderCallbacks(), value, (e) => {
                        var notifiedFolder = folder.getFolderFromFilePath(e.File);
                        var match: string[] = null;
                        if (notifiedFolder && notifiedFolder.path) {
                            match = notifiedFolder.path.match(key);
                        }
                        return match && match.length > 0;
                    });
                }
            } else if (type === "ConfigurationChangeNotification") {
                if (value.Name.indexOf("Raven/Synchronization/Destinations") >= 0) {
                    this.fireEvents(this.allFsDestinationsHandlers(), value, (e) => true);
                }
                this.fireEvents(this.allFsConfigHandlers(), value, (e) => true);
            }
            else {
                console.log("Unhandled Changes API notification type: " + type);
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

    watchAllReplicationConflicts(onChange: (e: replicationConflictNotificationDto) => void) {
        var callback = new changesCallback<replicationConflictNotificationDto>(onChange);
        if (this.allReplicationConflicts().length == 0) {
            this.send('watch-replication-conflicts');
        }
        this.allReplicationConflicts.push(callback);
        return new changeSubscription(() => {
            this.allReplicationConflicts.remove(callback);
            if (this.allReplicationConflicts().length == 0) {
                this.send('unwatch-replication-conflicts');
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
            if (this.watchedPrefixes[docIdPrefix]().length == 0) {
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

    watchFsSync(onChange: (e: synchronizationUpdateNotification) => void): changeSubscription {
        var callback = new changesCallback<synchronizationUpdateNotification>(onChange);
        if (this.allFsSyncHandlers().length == 0) {
            this.send('watch-sync');
        }
        this.allFsSyncHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsSyncHandlers.remove(callback);
            if (this.allFsSyncHandlers().length == 0) {
                this.send('unwatch-sync');
            }
        });
    }

    watchFsConflicts(onChange: (e: synchronizationConflictNotification) => void) : changeSubscription {
        var callback = new changesCallback<synchronizationConflictNotification>(onChange);
        if (this.allFsConflictsHandlers().length == 0) {
            this.send('watch-conflicts');
        }
        this.allFsConflictsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsConflictsHandlers.remove(callback);
            if (this.allFsConflictsHandlers().length == 0) {
                this.send('unwatch-conflicts');
            }
        });
    }

    watchFsFolders(folder: string, onChange: (e: fileChangeNotification) => void): changeSubscription {
        var callback = new changesCallback<fileChangeNotification>(onChange);
        if (typeof (this.watchedFolders[folder]) === "undefined") {
            this.send('watch-folder', folder);
            this.watchedFolders[folder] = ko.observableArray();
        }
        this.watchedFolders[folder].push(callback);
        return new changeSubscription(() => {
            this.watchedFolders[folder].remove(callback);
            if (this.watchedFolders[folder].length == 0) {
                delete this.watchedFolders[folder];
                this.send('unwatch-folder', folder);
            }
        });
    }

    watchFsConfig(onChange: (e: filesystemConfigNotification) => void): changeSubscription {
        var callback = new changesCallback<filesystemConfigNotification>(onChange);
        if (this.allFsConfigHandlers().length == 0) {
            this.send('watch-config');
        }
        this.allFsConfigHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsConfigHandlers.remove(callback);
            if (this.allFsConfigHandlers().length == 0) {
                this.send('unwatch-config');
            }
        });
    }

    watchFsDestinations(onChange: (e: filesystemConfigNotification) => void): changeSubscription {
        var callback = new changesCallback<filesystemConfigNotification>(onChange);
        if (this.allFsDestinationsHandlers().length == 0) {
            this.send('watch-config');
        }
        this.allFsDestinationsHandlers.push(callback);
        return new changeSubscription(() => {
            this.allFsDestinationsHandlers.remove(callback);
            if (this.allFsDestinationsHandlers().length == 0) {
                this.send('unwatch-config');
            }
        });
    }
    
    dispose() {
        this.connectToChangesApiTask.done(() => {
            var isCloseNeeded: boolean;

            if (isCloseNeeded = this.webSocket && this.webSocket.readyState == this.readyStateOpen){
                console.log("Disconnecting from WebSocket changes API for (rs = " + this.rs.name + ")");
                this.webSocket.close(this.normalClosureCode, this.normalClosureMessage);
            }
            else if (isCloseNeeded = this.eventSource && this.eventSource.readyState == this.readyStateOpen) {
                console.log("Disconnecting from EventSource changes API for (rs = " + this.rs.name + ")");
                this.eventSource.close();
            }

            if (isCloseNeeded) {
                this.send('disconnect', undefined, false);
                this.isCleanClose = true;
            }
        });
    }

    public getResourceName() {
        return this.rs.name;
    }
}

export = changesApi;