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
    detailedDescription = ko.observable<Raven.Client.Documents.Operations.IOperationDetailedDescription>();
    
    endTime = ko.observable<moment.Moment>();

    startTimeForOperation = ko.observable<moment.Moment>();
    startTimeForDocuments = ko.observable<moment.Moment>();
    startTimeForRevisions = ko.observable<moment.Moment>();
    startTimeForCounters = ko.observable<moment.Moment>();
    
    durationForOperation: KnockoutComputed<string>;
    durationInSecondsForOperation: KnockoutComputed<number>;
    durationInSecondsDocuments: KnockoutComputed<number>;
    durationInSecondsRevisions: KnockoutComputed<number>;
    durationInSecondsCounters: KnockoutComputed<number>;
    
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
                
        this.startTimeForOperation(incomingChanges.StartTime ? serverTime.default.getAdjustedTime(moment.utc(incomingChanges.StartTime)) : null);
        this.endTime(incomingChanges.EndTime ?serverTime.default.getAdjustedTime(moment.utc(incomingChanges.EndTime)) : null);
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
            const start = this.startTimeForOperation();
            const end = this.endTime();
            const created = this.createdAt();
            const dateToUse = end || start || created;
            return moment(dateToUse).local();
        });

        this.durationForOperation = ko.pureComputed(() => {
            return generalUtils.formatAsTimeSpan(this.getDuration(this.startTimeForOperation()));
        });

        this.durationInSecondsForOperation = ko.pureComputed(() => {
            return this.getDuration(this.startTimeForOperation()) / 1000;
        });
        
        this.durationInSecondsDocuments = ko.pureComputed(() => {
            return this.getDuration(this.startTimeForDocuments()) / 1000;
        });
        this.durationInSecondsRevisions = ko.pureComputed(() => {
            return this.getDuration(this.startTimeForRevisions()) / 1000;
        });
        this.durationInSecondsCounters = ko.pureComputed(() => {
            return this.getDuration(this.startTimeForCounters()) / 1000;
        });
    }

    getDuration(start: moment.Moment) {
        const end = this.endTime();
        const endTime = end || serverTime.default.getAdjustedTime(timeHelpers.utcNowWithSecondPrecision());

        return Math.max(endTime.diff(start), 0);
    }
    
    public setStartTimeForDocuments(startTime: string) {
        if (!this.startTimeForDocuments()) {  
            this.startTimeForDocuments(serverTime.default.getAdjustedTime(moment.utc(startTime)));
        }
    }
    
    public setStartTimeForRevisions(startTime: string) {
        if (!this.startTimeForRevisions()) {
            this.startTimeForRevisions(serverTime.default.getAdjustedTime(moment.utc(startTime)));
        }
    }
    
    public setStartTimeForCounters(startTime: string) {
        if (!this.startTimeForCounters()) {
            this.startTimeForCounters(serverTime.default.getAdjustedTime(moment.utc(startTime)));
        }
    }
}

export = operation;
