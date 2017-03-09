/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");
import database = require("models/resources/database");

class operation extends abstractNotification {

    operationId = ko.observable<number>();
    progress = ko.observable<Raven.Client.Documents.Operations.IOperationProgress>();
    result = ko.observable<Raven.Client.Documents.Operations.IOperationResult>();
    status = ko.observable<Raven.Client.Documents.Operations.OperationStatus>();
    killable = ko.observable<boolean>();

    isCompleted: KnockoutComputed<boolean>;
    isPercentageProgress: KnockoutComputed<boolean>;

    constructor(db: database, dto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        super(db, dto);

        this.operationId(dto.OperationId);
        this.updateWith(dto);
        this.initializeObservables();
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        super.updateWith(incomingChanges);

        this.killable(incomingChanges.Killable);

        const stateDto = incomingChanges.State;
        this.progress(stateDto.Progress);
        this.result(stateDto.Result);
        this.status(stateDto.Status);
    }

    percentageProgress(): number {
        const progress = this.progress() as Raven.Client.Documents.Operations.DeterminateProgress;
        return Math.round(progress.Processed * 100.0 / progress.Total);
    }

    private initializeObservables() {
        this.isCompleted = ko.pureComputed(() => this.status() !== "InProgress");
        this.hasDetails = ko.pureComputed(() => {
            const hasResult = !!this.result();
            const hasProgress = !!this.progress();
            return hasResult || hasProgress;
        });
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
