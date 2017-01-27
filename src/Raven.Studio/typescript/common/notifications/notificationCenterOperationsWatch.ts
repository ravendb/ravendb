import resource = require("models/resources/resource");
import database = require("models/resources/database");

import EVENTS = require("common/constants/events");
import messagePublisher = require("common/messagePublisher");
import changesApi = require("common/changesApi");
import changesContext = require("common/changesContext");

import killOperationCommand = require("commands/operations/killOperationCommand");

class notificationCenterOperationsWatch {

    private resource: resource;

    private operations = new Map<number, JQueryDeferred<Raven.Client.Data.IOperationResult>>();
    private watchedProgresses = new Map<number, Array<(progress: Raven.Client.Data.IOperationProgress) => void>>();

    //TODO: notify about connection/disconnection

    configureFor(resource: resource) {
        this.resource = resource;
        this.operations.clear();
        this.watchedProgresses.clear();
    }

    monitorOperation<TProgress extends Raven.Client.Data.IOperationProgress, TResult extends Raven.Client.Data.IOperationResult>
        (operationId: number, onProgress: (progress: TProgress) => void = null): JQueryPromise<TResult> {

        if (onProgress) {
            let progresses = this.watchedProgresses.get(operationId);
            if (!progresses) {
                progresses = [] as Array<(progress: Raven.Client.Data.IOperationProgress) => void>;
                this.watchedProgresses.set(operationId, progresses);
            }

            progresses.push(onProgress);
        }

        return this.getOrCreateOperation(operationId).promise();
    }

    private getOrCreateOperation(operationId: number): JQueryDeferred<Raven.Client.Data.IOperationResult> {
        if (this.operations.has(operationId)) {
            return this.operations.get(operationId);
        } else {
            const task = $.Deferred<Raven.Client.Data.IOperationResult>();
            this.operations.set(operationId, task);
            return task;
        }
    }

    onOperationChange(operationDto: Raven.Server.NotificationCenter.Actions.OperationChanged) {
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
            } else {
                operation.reject(state.Result);
            }
        }
    }

}

export = notificationCenterOperationsWatch;