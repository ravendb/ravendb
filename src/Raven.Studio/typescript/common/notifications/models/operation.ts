/// <reference path="../../../../typings/tsd.d.ts" />
import serverTime = require("common/helpers/database/serverTime");
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
    taskType = ko.observable<Raven.Server.Documents.Operations.Operations.OperationType>();
    
    startTime = ko.observable<moment.Moment>();
    endTime = ko.observable<moment.Moment>();
    duration: KnockoutComputed<string>;
    durationInSeconds: KnockoutComputed<number>;

    isCompleted: KnockoutComputed<boolean>;
    isCanceled: KnockoutComputed<boolean>;
    isPercentageProgress: KnockoutComputed<boolean>;
    headerIconAddonClass: KnockoutComputed<string>;

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
            return generalUtils.formatAsTimeSpan(this.getDuration());
        });

        this.durationInSeconds = ko.pureComputed(() => {
            return this.getDuration() / 1000;
        });
    }

    private getDuration() {
        const start = this.startTime();
        const end = this.endTime();

        // Adjust studio-server time difference if we are 'in progress' 
        const endTime = end || serverTime.default.getAdjustedTime(timeHelpers.utcNowWithSecondPrecision());

        return endTime.diff(start);
    }
}

export = operation;
