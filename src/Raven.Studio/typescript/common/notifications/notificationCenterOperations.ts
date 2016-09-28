import resource = require("models/resources/resource");
import database = require("models/resources/database");

import watchedOperation = require("common/notifications/watchedOperation");
import notificationCenterPersistanceStorage = require("common/notifications/notificationCenterPersistanceStorage");
import messagePublisher = require("common/messagePublisher");
import changesApi = require("common/changesApi");
import changesContext = require("common/changesContext");

import getOperationCommand = require("commands/operations/getOperationCommand");
import dismissOperationCommand = require("commands/operations/dismissOperationCommand");
import getRunningTasksCommand = require("commands/operations/getRunningTasksCommand");
import killOperationCommand = require("commands/operations/killOperationCommand");

class notificationCenterOperations {
    watchedOperations = ko.observableArray<watchedOperation>([]);
    storage: notificationCenterPersistanceStorage;
    serverTime = ko.observable<string>();
    activeChangesApi = ko.observable<changesApi>();

    constructor() {
        this.storage = new notificationCenterPersistanceStorage(this.serverTime);
        this.activeChangesApi = changesContext.currentResourceChangesApi;
        ko.postbox.subscribe("ChangesApiReconnected", () => this.onReconnect());
        $(window).bind("storage", (event) => this.onStorageEvent(event));
    }

    private onStorageEvent(event: JQueryEventObject) {
        const storageEvent = event.originalEvent as StorageEvent;

        const rs = this.activeChangesApi().getResource();

        if (!this.storage.isStorageKeyMatch(rs, storageEvent)) {
            return;
        }

        const oldValue = JSON.parse(storageEvent.oldValue) as localStorageOperationsDto;
        const oldOperations = oldValue ? oldValue.Operations : [];

        const newValue = JSON.parse(storageEvent.newValue) as localStorageOperationsDto;
        const newOperations = newValue ? newValue.Operations : [];

        const newOperationIds = newOperations.filter(x => oldOperations.indexOf(x) < 0);
        const dismissedOperationIds = oldOperations.filter(x => newOperations.indexOf(x) < 0);

        newOperationIds.forEach(id => {
            this.monitorOperation(rs, id, null);
        });

        dismissedOperationIds.forEach(id => {
            this.dismissOperation(id, false);
        });
    }

    private onReconnect() {
        const changes = this.activeChangesApi();
        const rs = changes.getResource();

        this.serverTime(changes.serverStartTime());
        this.watchedOperations.removeAll();

        const operationIds = this.storage.loadOperations(changes.getResource());

        new getRunningTasksCommand(rs as database)
            .execute()
            .done((operations: Raven.Server.Documents.PendingOperation[]) => {
                operationIds.forEach(operationId => {
                    var matchedOp = operations.first(x => x.Id === operationId);
                    if (matchedOp) {
                        this.subscribeForOperation(operationId, n => this.onStatus(rs, n.OperationId, n.State));
                        this.onOperationInfo(matchedOp, operationId);
                    }
                });
            })
            .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to get running tasks", response.responseText, response.statusText));
    }

    monitorOperation<TProgress extends Raven.Client.Data.IOperationProgress, TResult extends Raven.Client.Data.IOperationResult>
        (rs: resource, operationId: number, onProgress: (progress: TProgress) => void = null): JQueryPromise<TResult> {
        const task = $.Deferred<TResult>();

        this.subscribeForOperation(operationId, notification => this.onStatus(rs, notification.OperationId, notification.State, onProgress, task));

        this.storage.saveOperations(rs, this.getOperationIds());

        this.fetchOperationWithRetries(rs, operationId, 3)
            .done(operation => this.onOperationInfo(operation, operationId))
            .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to get operation", response.responseText, response.statusText));

        return task.promise();
    }

    private subscribeForOperation(operationId: number, onChange: (e: Raven.Client.Data.OperationStatusChangeNotification) => void) {
        const disposeHandle = changesContext.currentResourceChangesApi().watchOperation(operationId, onChange);
        const watchedOp = new watchedOperation(operationId, disposeHandle);
        this.watchedOperations.unshift(watchedOp);
    }

    killOperation(operationId: number) {
        const rs = this.activeChangesApi().getResource();

        new killOperationCommand(rs as database, operationId).execute()
            .done(); //TODO mark as kill sent 
    }

    dismissOperation(operationId: number, saveOperations: boolean = true) {
        const watchedOperation = this.getWatchedOperationById(operationId);

        const rs = this.activeChangesApi().getResource();

        if (watchedOperation) {
            new dismissOperationCommand(rs, operationId)
                .execute(); // we don't care about successful completion here

            this.watchedOperations.remove(watchedOperation);

            if (saveOperations) {
                this.storage.saveOperations(rs, this.getOperationIds());
            }
        }
    }

    private onOperationInfo(operation: Raven.Server.Documents.PendingOperation, operationId: number) {
        const watchedOperation = this.getWatchedOperationById(operationId);
        if (watchedOperation) {
            watchedOperation.onInfoFetched(operation);
        }
    }

    private getWatchedOperationById(operationId: number) {
        return this.watchedOperations.first(x => x.operationId === operationId);
    }

    private onStatus(rs: resource, operationId: number, state: Raven.Client.Data.OperationState, onProgress: (ProgressEvent: Raven.Client.Data.IOperationProgress) => void = null, task: JQueryDeferred<Raven.Client.Data.IOperationResult> = null) {
        const watchedOperation = this.getWatchedOperationById(operationId);
        if (watchedOperation) {
            watchedOperation.onStatus(state);
        }

        // now handle optional parameters
        if (state.Status === "InProgress") {
            if (onProgress) {
                onProgress((state.Progress));
            }
        } else { // operation is completed
            if (task) {
                if (state.Status === "Completed") {
                    task.resolve(state.Result);
                } else {
                    task.reject(state.Result);
                }
            }
        }
    }

    private fetchOperationWithRetries(rs: resource, operationId: number, retries = 3): JQueryPromise<Raven.Server.Documents.PendingOperation> {
        const task = $.Deferred<Raven.Server.Documents.PendingOperation>();
        this.fetchOperation(rs, operationId, 3, task);
        return task;
    }

    private fetchOperation(rs: resource, operationId: number, retries: number, task: JQueryDeferred<Raven.Server.Documents.PendingOperation>) {
        new getOperationCommand(rs, operationId)
            .execute()
            .done(op => task.resolve(op))
            .fail((response: JQueryXHR) => {
                if (retries === 0) {
                    task.reject(response);
                    return;
                }

                setTimeout(() => this.fetchOperation(rs, operationId, retries - 1, task), 200);
            });
    }

    private getOperationIds() {
        return this.watchedOperations().map(x => x.operationId);
    }

}

export = notificationCenterOperations;