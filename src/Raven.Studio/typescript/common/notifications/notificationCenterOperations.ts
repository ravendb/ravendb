import resource = require("models/resources/resource");
import database = require("models/resources/database");

import EVENTS = require("common/constants/events");
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
        this.activeChangesApi = changesContext.default.currentResourceChangesApi;
        ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, () => this.onReconnect());
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
                    var matchedOp = operations.find(x => x.Id === operationId);
                    if (matchedOp) {
                        this.subscribeForOperation(operationId, n => this.onStatus(rs, n.OperationId, n.State)); //TODO: shold we pass task and onProgres here?
                        this.onOperationInfo(rs, matchedOp, operationId, null, null); //TODO: pass task, and on progress here
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

        this.fetchOperationWithRetries(rs, operationId, 5)
            .done(operation => this.onOperationInfo(rs, operation, operationId, onProgress, task))
            .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to get operation", response.responseText, response.statusText));

        return task.promise();
    }

    private subscribeForOperation(operationId: number, onChange: (e: Raven.Client.Data.OperationStatusChangeNotification) => void) {
        const disposeHandle = this.activeChangesApi().watchOperation(operationId, onChange);
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

    private onOperationInfo<TProgress extends Raven.Client.Data.IOperationProgress, TResult extends Raven.Client.Data.IOperationResult>(
        rs: resource, operation: Raven.Server.Documents.PendingOperation,
        operationId: number, onProgress: (ProgressEvent: TProgress) => void = null, task: JQueryDeferred<TResult>) {

        const watchedOperation = this.getWatchedOperationById(operationId);
        if (watchedOperation) {
            watchedOperation.onInfoFetched(operation);
        }

        this.onStatus(rs, operationId, operation.State, onProgress, task);
    }

    private getWatchedOperationById(operationId: number) {
        return this.watchedOperations().find(x => x.operationId === operationId);
    }

    private onStatus<TProgress extends Raven.Client.Data.IOperationProgress, TResult extends Raven.Client.Data.IOperationResult>(
        rs: resource, operationId: number, state: Raven.Client.Data.OperationState,
        onProgress: (ProgressEvent: TProgress) => void = null,
        task: JQueryDeferred<TResult> = null) {
        const watchedOperation = this.getWatchedOperationById(operationId);
        if (watchedOperation) {
            watchedOperation.onStatus(state);
        }

        // now handle optional parameters
        if (state.Status === "InProgress") {
            if (onProgress) {
                onProgress(state.Progress as TProgress);
            }
        } else { // operation is completed
            if (task) {
                if (state.Status === "Completed") {
                    task.resolve(state.Result as TResult);
                } else {
                    task.reject(state.Result);
                }
            }
        }
    }

    private fetchOperationWithRetries(rs: resource, operationId: number, retries = 3): JQueryPromise<Raven.Server.Documents.PendingOperation> {
        const task = $.Deferred<Raven.Server.Documents.PendingOperation>();
        this.fetchOperation(rs, operationId, retries, task);
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

                setTimeout(() => this.fetchOperation(rs, operationId, retries - 1, task), 500);
            });
    }

    private getOperationIds() {
        return this.watchedOperations().map(x => x.operationId);
    }

}

export = notificationCenterOperations;