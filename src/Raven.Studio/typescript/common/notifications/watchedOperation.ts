import changeSubscription = require("common/changeSubscription");

class watchedOperation {
    private disposeHandle: changeSubscription;

    operationId: number;

    state = ko.observable<Raven.Client.Data.OperationState>();
    description = ko.observable<Raven.Server.Documents.PendingOperationDescription>();
    killable = ko.observable<boolean>(false);
    completed: KnockoutComputed<boolean>;
    visible: KnockoutComputed<boolean>;

    isSuccess: KnockoutComputed<boolean>;
    isFailure: KnockoutComputed<boolean>;
    isCancelled: KnockoutComputed<boolean>;

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
        this.completed = ko.pureComputed(() => {
            var state = this.state();
            if (state) {
                return state.Status !== "InProgress";
            }
            return false;
        });

        this.isSuccess = this.statusComparer("Completed");
        this.isCancelled = this.statusComparer("Canceled");
        this.isFailure = this.statusComparer("Faulted");
    }

    private statusComparer(desiredStatus: Raven.Client.Data.OperationStatus): KnockoutComputed<boolean> {
        return ko.pureComputed(() => {
            var state = this.state();
            if (state) {
                return state.Status === desiredStatus;
            }
            return false;
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
    }

}

export = watchedOperation;