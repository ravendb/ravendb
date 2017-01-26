/// <reference path="../../../../typings/tsd.d.ts" />

import abstractAction = require("common/notifications/actions/abstractAction");
import resource = require("models/resources/resource");

class operation extends abstractAction {

    operationId = ko.observable<number>();
    progress = ko.observable<Raven.Client.Data.IOperationProgress>();
    result = ko.observable<Raven.Client.Data.IOperationResult>();
    status = ko.observable<Raven.Client.Data.OperationStatus>();
    killable = ko.observable<boolean>();

    isSuccess: KnockoutComputed<boolean>;
    isFailure: KnockoutComputed<boolean>;
    isCancelled: KnockoutComputed<boolean>;
    isCompleted: KnockoutComputed<boolean>;
    isPercentageProgress: KnockoutComputed<boolean>;

    constructor(resource: resource, dto: Raven.Server.NotificationCenter.Actions.OperationChanged) {
        super(resource, dto);

        this.operationId(dto.OperationId);
        this.updateWith(dto);
        this.initializeObservables();
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Actions.OperationChanged) {
        super.updateWith(incomingChanges);

        this.killable(incomingChanges.Killable);

        const stateDto = incomingChanges.State;
        this.progress(stateDto.Progress);
        this.result(stateDto.Result);
        this.status(stateDto.Status);
    }

    percentageProgress(): number {
        const progress = this.progress() as Raven.Client.Data.DeterminateProgress;
        return Math.round(progress.Processed * 100.0 / progress.Total);
    }

    private initializeObservables() {
        this.isSuccess = ko.pureComputed(() => this.status() === "Completed");
        this.isCancelled = ko.pureComputed(() => this.status() === "Canceled");
        this.isFailure = ko.pureComputed(() => this.status() === "Faulted");
        this.isCompleted = ko.pureComputed(() => this.status() !== "InProgress");
        this.isPercentageProgress = ko.pureComputed(() => {
            const progress = this.progress();

            if (this.isCompleted() || !progress) {
                return false;
            }

            return progress.hasOwnProperty("Processed") && progress.hasOwnProperty("Total");
        });
    }

}

export = operation;
