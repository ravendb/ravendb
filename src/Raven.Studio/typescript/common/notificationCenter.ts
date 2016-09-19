import resource = require("models/resources/resource");
import changesContext = require("common/changesContext");
import getOperationStatusCommand = require("commands/operations/getOperationStatusCommand");
import changeSubscription = require("common/changeSubscription");

//TODO: handle server restart - insert server start time on local storage?

type watchedOperation = {
    disposeHandle: changeSubscription;
}

class notificationCenter {
    static instance = new notificationCenter();

    private watchedOperationsPerDatabase = new Map<string, Map<number, watchedOperation>>();

    currentlyConnectedChangesApi = changesContext.currentResourceChangesApi;

    monitorOperation<TProgress extends Raven.Client.Data.IOperationProgress, TResult extends Raven.Client.Data.IOperationResult>
        (rs: resource, operationId: number, onProgress: (progress: TProgress) => void = null): JQueryPromise<TResult> {
        const task = $.Deferred<TResult>();
        
        
        if (this.currentlyConnectedChangesApi().getResourceName() !== rs.name) {
            throw new Error("Operation monitoring can not be configured. We are connected to different changes API");
        }

        const disposeHandle = this.currentlyConnectedChangesApi().watchOperation(operationId, notification => this.onStatus(rs, notification.OperationId, notification.State, onProgress, task));
        this.markAsWatched(rs, operationId, disposeHandle);

        this.fetchOperationStatus(rs, operationId)
            .done(state => this.onStatus(rs, operationId, state, onProgress, task));
            //TODO: handle failure (and 404)

        return task.promise();
    }    

    private onStatus(rs: resource, operationId: number, state: Raven.Client.Data.OperationState, onProgress: (ProgressEvent: Raven.Client.Data.IOperationProgress) => void, task: JQueryDeferred<Raven.Client.Data.IOperationResult>) {
        switch (state.Status) {
            case "InProgress":
                this.onProgress(state, onProgress);
                break;
            case "Canceled":
                this.stopWatching(rs, operationId);
                this.onCanceled(state, task);
                break;
            case "Faulted":
                this.stopWatching(rs, operationId);
                this.onFailure(state, task);
                break;
            case "Completed":
                this.stopWatching(rs, operationId);
                this.onSuccess(state, task);
                break;
        }
    }

    private onProgress(state: Raven.Client.Data.OperationState, onProgress: (ProgressEvent: Raven.Client.Data.IOperationProgress) => void) {
        onProgress(state.Progress);
    }

    private onCanceled(state: Raven.Client.Data.OperationState, task: JQueryDeferred<Raven.Client.Data.IOperationResult>) {
        task.reject(state.Result);
    }

    private onFailure(state: Raven.Client.Data.OperationState, task: JQueryDeferred<Raven.Client.Data.IOperationResult>) {
        task.reject(state.Result);
    }

    private onSuccess(state: Raven.Client.Data.OperationState, task: JQueryDeferred<Raven.Client.Data.IOperationResult>) {
        task.resolve(state.Result);
    }

    private fetchOperationStatus(rs: resource, operationId: number): JQueryPromise<Raven.Client.Data.OperationState> {
        return new getOperationStatusCommand(rs, operationId).execute();
    }

    private storageKey(rs: resource) {
        return rs.fullTypeName + "_" + rs.name;
    }

    private markAsWatched(rs: resource, operationId: number, disposeHandle: changeSubscription) {
        const key = this.storageKey(rs);
        if (!this.watchedOperationsPerDatabase.has(key)) {
            this.watchedOperationsPerDatabase.set(key, new Map<number, watchedOperation>());
        }

        this.watchedOperationsPerDatabase.get(key).set(operationId,
        {
            disposeHandle: disposeHandle
        });
    }

    private stopWatching(rs: resource, operationId: number) {
        const key = this.storageKey(rs);
        if (this.watchedOperationsPerDatabase.has(key)) {
            const watchedMap = this.watchedOperationsPerDatabase.get(key);
            if (watchedMap.has(operationId)) {
                const watchedOp = watchedMap.get(operationId);
                watchedOp.disposeHandle.off();
                watchedMap.delete(operationId);
            } else {
                console.error(`I'm not watching ${operationId} in ${key}`);
            }
        } else {
            console.error(`I'm not watching any operation in: ${key}`);
        }
    }
}

export = notificationCenter;