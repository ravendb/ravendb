/// <reference path="../../../../typings/tsd.d.ts" />
import serverTime = require("common/helpers/database/serverTime");
import abstractNotification = require("common/notifications/models/abstractNotification");
import database = require("models/resources/database");
import generalUtils = require("common/generalUtils");
import timeHelpers = require("common/timeHelpers");
import moment = require("moment");

class operation extends abstractNotification {

    operationId = ko.observable<number>();
    progress = ko.observable<Raven.Client.Documents.Operations.IOperationProgress>();
    result = ko.observable<Raven.Client.Documents.Operations.IOperationResult>();
    status = ko.observable<Raven.Client.Documents.Operations.OperationStatus>();
    killable = ko.observable<boolean>();
    taskType = ko.observable<Raven.Server.Documents.Operations.Operations.OperationType>();
    detailedDescription = ko.observable<Raven.Client.Documents.Operations.IOperationDetailedDescription>();
    
    startTime = ko.observable<moment.Moment>();
    endTime = ko.observable<moment.Moment>();
    
    duration: KnockoutComputed<string>;
    durationInSeconds: KnockoutComputed<number>;
    
    isCompleted: KnockoutComputed<boolean>;
    isCanceled: KnockoutComputed<boolean>;
    isPercentageProgress: KnockoutComputed<boolean>;
    onUpdateCallbacks: Array<() => void> = [];
    headerIconAddonClass: KnockoutComputed<string>;

    constructor(db: database, dto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        super(db, dto);

        this.operationId(dto.OperationId);
        this.updateWith(dto);
        this.initializeObservables();
    }
    
    invokeOnUpdateHandlers() {
        this.onUpdateCallbacks.forEach(c => c());
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        super.updateWith(incomingChanges);

        this.killable(incomingChanges.Killable);

        const stateDto = incomingChanges.State;
        this.progress(stateDto.Progress);
        this.result(stateDto.Result);
        this.status(stateDto.Status);
        this.taskType(incomingChanges.TaskType);
        this.detailedDescription(incomingChanges.DetailedDescription);
                
        this.startTime(incomingChanges.StartTime ? serverTime.default.getAdjustedTime(moment.utc(incomingChanges.StartTime)) : null);
        this.endTime(incomingChanges.EndTime ? serverTime.default.getAdjustedTime(moment.utc(incomingChanges.EndTime)) : null);
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
        
        this.headerIconAddonClass = ko.pureComputed(() => {
            switch (this.status()) {
                case "Completed":
                    return "icon-addon-tick";
                case "Faulted":
                    return "icon-addon-danger";
                case "Canceled":
                    return "icon-addon-warning";
                default:
                    return null;
            }
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
            return generalUtils.formatAsTimeSpan(this.getDuration(this.startTime()));
        });

        this.durationInSeconds = ko.pureComputed(() => {
            return this.getDuration(this.startTime()) / 1000;
        });
    }

    getDuration(start: moment.Moment) {
        const end = this.endTime();
        const endTime = end || serverTime.default.getAdjustedTime(timeHelpers.utcNowWithSecondPrecision());

        return Math.max(endTime.diff(start), 0);
    }
    
    public getElapsedSeconds(startTime: string) {
        const adjustedStartTime = serverTime.default.getAdjustedTime(moment.utc(startTime));
        return this.getDuration(adjustedStartTime) / 1000;
    }
}

export = operation;
