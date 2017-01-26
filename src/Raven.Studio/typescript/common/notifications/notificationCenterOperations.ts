import resource = require("models/resources/resource");
import database = require("models/resources/database");

import EVENTS = require("common/constants/events");
import messagePublisher = require("common/messagePublisher");
import changesApi = require("common/changesApi");
import changesContext = require("common/changesContext");

import killOperationCommand = require("commands/operations/killOperationCommand");

class notificationCenterOperations {

    activeChangesApi = ko.observable<changesApi>();
    /* TODO: 
    watchedOperations = ko.observableArray<watchedOperation>([]);
    

    constructor() {
        this.activeChangesApi = changesContext.default.resourceChangesApi; //TODO: no needed here?
        ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, () => this.onReconnect());
    }*/

    private onReconnect() {
        const changes = this.activeChangesApi();
        const rs = changes.getResource();

        /* TODO
        this.watchedOperations.removeAll();

        const operationIds = this.storage.loadOperations(changes.getResource());

        new getOperationsCommand(rs as database)
            .execute()
            .done((operations: Raven.Server.Documents.PendingOperation[]) => { //TOOD: -> operation
                operationIds.forEach(operationId => {
                    var matchedOp = operations.find(x => x.Id === operationId);
                    if (matchedOp) {
                        this.subscribeForOperation(operationId, n => this.onStatus(rs, n.OperationId, n.State)); //TODO: shold we pass task and onProgres here?
                        this.onOperationInfo(rs, matchedOp, operationId, null, null); //TODO: pass task, and on progress here
                    }
                });
            })
            .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to get running tasks", response.responseText, response.statusText));*/
    }

    monitorOperation<TProgress extends Raven.Client.Data.IOperationProgress, TResult extends Raven.Client.Data.IOperationResult>
        (rs: resource, operationId: number, onProgress: (progress: TProgress) => void = null): JQueryPromise<TResult> {
        const task = $.Deferred<TResult>();

        //TODO: this.subscribeForOperation(operationId, notification => this.onStatus(rs, notification.OperationId, notification.State, onProgress, task));

        /* TODO
        this.fetchOperationWithRetries(rs, operationId, 5)
            .done(operation => this.onOperationInfo(rs, operation, operationId, onProgress, task))
            .fail((response: JQueryXHR) => messagePublisher.reportError("Failed to get operation", response.responseText, response.statusText));
        */

        return task.promise();
    }

    private subscribeForOperation(operationId: number, onChange: (e: Raven.Client.Data.OperationStatusChangeNotification) => void) {
        /* TODO no need for doing this
        const disposeHandle = this.activeChangesApi().watchOperation(operationId, onChange);
        const watchedOp = new watchedOperation(operationId, disposeHandle);
        this.watchedOperations.unshift(watchedOp);*/
    }

    killOperation(operationId: number) {
        const rs = this.activeChangesApi().getResource();

        new killOperationCommand(rs as database, operationId).execute()
            .done(); //TODO mark as kill sent 
    }

    dismissOperation(operationId: number, saveOperations: boolean = true) {
        /* TODO
        const watchedOperation = this.getWatchedOperationById(operationId);

        const rs = this.activeChangesApi().getResource();

        if (watchedOperation) {
            new dismissOperationCommand(rs, operationId)
                .execute(); // we don't care about successful completion here

            this.watchedOperations.remove(watchedOperation);

            if (saveOperations) {
                this.storage.saveOperations(rs, this.getOperationIds());
            }
        }*/
    }

    /* TODO
    private onOperationInfo<TProgress extends Raven.Client.Data.IOperationProgress, TResult extends Raven.Client.Data.IOperationResult>(
        rs: resource, operation: Raven.Server.Documents.PendingOperation,
        operationId: number, onProgress: (ProgressEvent: TProgress) => void = null, task: JQueryDeferred<TResult>) {

        const watchedOperation = this.getWatchedOperationById(operationId);
        if (watchedOperation) {
            watchedOperation.onInfoFetched(operation);
        }

        this.onStatus(rs, operationId, operation.State, onProgress, task);
    }*/

    private getWatchedOperationById(operationId: number) {
        //TODO: return this.watchedOperations().find(x => x.operationId === operationId);
    }
    /* TODO
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
    }*/

}

export = notificationCenterOperations;