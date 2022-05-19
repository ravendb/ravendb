import database = require("models/resources/database");

class notificationCenterOperationsWatch {

    private db: database;

    private operations = new Map<number, JQueryDeferred<unknown>>();
    private watchedProgresses = new Map<number, Array<(progress: unknown) => void>>();

    configureFor(db: database) {
        this.db = db;
        this.operations.clear();
        this.watchedProgresses.clear();
    }

    monitorOperation(operationId: number, onProgress: (progress: unknown) => void = null): JQueryPromise<unknown> {
        if (onProgress) {
            let progresses = this.watchedProgresses.get(operationId);
            if (!progresses) {
                progresses = [];
                this.watchedProgresses.set(operationId, progresses);
            }

            progresses.push(onProgress);
        }

        return this.getOrCreateOperation(operationId).promise();
    }

    private getOrCreateOperation(operationId: number): JQueryDeferred<unknown> {
        if (this.operations.has(operationId)) {
            return this.operations.get(operationId) as JQueryDeferred<unknown>;
        } else {
            const task = $.Deferred<unknown>();
            this.operations.set(operationId, task);
            return task;
        }
    }

    onOperationChange(operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        const operationId = operationDto.OperationId;
        const state = operationDto.State;

        if (state.Status === "InProgress") {
            const progresses = this.watchedProgresses.get(operationId);

            if (progresses) {
                progresses.forEach(progress => {
                    progress(operationDto.State.Progress);
                });
            }
        } else { // handle completed message
            const operation = this.getOrCreateOperation(operationId);

            this.watchedProgresses.delete(operationId);

            if (state.Status === "Completed") {
                operation.resolve(state.Result);
            } else if (state.Status === "Canceled") {
                operation.reject({
                    Message: "The operation was canceled"
                } as Raven.Client.Documents.Operations.OperationExceptionResult);
            } else {
                operation.reject(state.Result);
            }
        }
    }

}

export = notificationCenterOperationsWatch;
