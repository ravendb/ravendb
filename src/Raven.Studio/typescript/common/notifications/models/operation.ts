/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");
import database = require("models/resources/database");
import generalUtils = require("common/generalUtils");
import timeHelpers = require("common/timeHelpers");

class operation extends abstractNotification {

    operationId = ko.observable<number>();
    progress = ko.observable<Raven.Client.Documents.Operations.IOperationProgress>();
    result = ko.observable<Raven.Client.Documents.Operations.IOperationResult>();
    status = ko.observable<Raven.Client.Documents.Operations.OperationStatus>();
    killable = ko.observable<boolean>();
    taskType = ko.observable<Raven.Server.Documents.Operations.DatabaseOperations.OperationType>();

    startTime = ko.observable<moment.Moment>();
    endTime = ko.observable<moment.Moment>();
    duration: KnockoutComputed<string>;

    isCompleted: KnockoutComputed<boolean>;
    isCanceled: KnockoutComputed<boolean>;
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
        this.taskType(incomingChanges.TaskType);
        this.startTime(incomingChanges.StartTime ? moment.utc(incomingChanges.StartTime) : null);
        this.endTime(incomingChanges.EndTime ? moment.utc(incomingChanges.EndTime) : null);
    }

    percentageProgress(): number {
        const progress = this.progress() as Raven.Client.Documents.Operations.DeterminateProgress;
        return Math.round(progress.Processed * 100.0 / progress.Total);
    }

    private initializeObservables() {
        this.isCompleted = ko.pureComputed(() => this.status() !== "InProgress");
        this.isCanceled = ko.pureComputed(() => this.status() === "Canceled");
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

        // override event date - for operations we use end date (if available), or start start
        this.displayDate = ko.pureComputed(() => {
            const start = this.startTime();
            const end = this.endTime();
            const created = this.createdAt();
            const dateToUse = end || start || created;
            return moment(dateToUse).local();
        });

        this.duration = ko.pureComputed(() => {
            const start = this.startTime();
            const end = this.endTime();

            const endTime = end || timeHelpers.utcNowWithSecondPrecision();

            return generalUtils.formatAsTimeSpan(endTime.diff(start));
        });
    }

}

export = operation;
