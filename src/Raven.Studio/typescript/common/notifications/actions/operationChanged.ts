/// <reference path="../../../../typings/tsd.d.ts" />

import abstractAction = require("common/notifications/actions/abstractAction");

class operationChanged extends abstractAction {

    operationId = ko.observable<number>();
    progress = ko.observable<Raven.Client.Data.IOperationProgress>();
    result = ko.observable<Raven.Client.Data.IOperationResult>();
    status = ko.observable<Raven.Client.Data.OperationStatus>();


    isSuccess: KnockoutComputed<boolean>;
    isFailure: KnockoutComputed<boolean>;
    isCancelled: KnockoutComputed<boolean>;
    isCompleted: KnockoutComputed<boolean>;

    constructor(dto: Raven.Server.NotificationCenter.Actions.OperationChanged) {
        super(dto);

        this.operationId(dto.OperationId);
        this.updateWith(dto);
        this.initializeObservables();
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Actions.OperationChanged) {
        super.updateWith(incomingChanges);

        const stateDto = incomingChanges.State;
        this.progress(stateDto.Progress);
        this.result(stateDto.Result);
        this.status(stateDto.Status);
    }

    private initializeObservables() {
        this.isSuccess = ko.pureComputed(() => this.status() === "Completed");
        this.isCancelled = ko.pureComputed(() => this.status() === "Canceled");
        this.isFailure = ko.pureComputed(() => this.status() === "Faulted");
        this.isCompleted = ko.pureComputed(() => this.status() !== "InProgress");
    }


     /* TODO:
    private disposeHandle: changeSubscription;

    operationId: number;

    state = ko.observable<Raven.Client.Data.OperationState>();
    description = ko.observable<Raven.Server.Documents.PendingOperationDescription>();
    killable = ko.observable<boolean>(false);
    visible: KnockoutComputed<boolean>;

    constructor(operationId: number, disposeHandle: changeSubscription) {
        this.operationId = operationId;
        this.disposeHandle = disposeHandle;
        this.initializeObservables();
    }

    private initializeObservables(): void {
        this.visible = ko.pureComputed(() => {
            const description = this.description();
            const state = this.state();
            return !!description && !!state;
        });
       
    }

    isPercentageProgress(): boolean {
        const state = this.state();
        if (state && state.Progress) {
            return state.Progress.hasOwnProperty("Processed") && state.Progress.hasOwnProperty("Total");
        }
        return false;
    }

    percentageProgress(): number {
        const progress = this.state().Progress as Raven.Client.Data.DeterminateProgress;
        return Math.round(progress.Processed * 100.0 / progress.Total);
    }

    onInfoFetched(operation: Raven.Server.Documents.PendingOperation): void {
        this.description(operation.Description);
        this.killable(operation.Killable);
        this.state(operation.State);
    }

    onStatus(state: Raven.Client.Data.OperationState): void {
        this.state(state);
        if (state.Status !== "InProgress") {
            this.disposeHandle.off();
        }
    }*/

}

export = operationChanged;
