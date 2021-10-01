import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import dialog = require("plugins/dialog");
import deleteTimeSeriesCommand = require("commands/database/documents/timeSeries/deleteTimeSeriesCommand");
import messagePublisher = require("common/messagePublisher");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import generalUtils = require("common/generalUtils");
import moment = require("moment");

class deleteTimeSeries extends dialogViewModelBase {

    view = require("views/database/timeSeries/deleteTimeSeries.html");

    spinners = {
        delete: ko.observable<boolean>(false)
    };

    datePickerOptions = {
        format: "YYYY-MM-DD HH:mm:ss.SSS",
        sideBySide: true
    };
    
    startDateLocal = ko.observable<moment.Moment>();
    endDateLocal = ko.observable<moment.Moment>();

    startDateUTC: KnockoutComputed<string>;
    endDateUTC: KnockoutComputed<string>;
    
    useMinStartDate = ko.observable<boolean>(false);
    useMaxEndDate = ko.observable<boolean>(false);
        
    showWarning: KnockoutComputed<boolean>;
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(private timeSeriesName: string, private documentId: string, private db: database, private criteria: timeSeriesDeleteCriteria) {
        super();
        criteria.selection = criteria.selection || [];
        
        this.startDateUTC = ko.pureComputed(() => {
            if (this.useMinStartDate()) {
                return null;
            }
            
            const newMoment = moment(this.startDateLocal());
            return newMoment.utc().format(generalUtils.utcFullDateFormat);
        });
        
        this.endDateUTC = ko.pureComputed(() => {
            if (this.useMaxEndDate()) {
                return null;
            }

            const newMoment = moment(this.endDateLocal());
            return newMoment.utc().format(generalUtils.utcFullDateFormat);
        });
        
        this.showWarning = ko.pureComputed(() => {
            const startDefined = this.useMinStartDate() || (this.startDateLocal() && this.startDateLocal.isValid());
            const endDefined = this.useMaxEndDate() || (this.endDateLocal() && this.endDateLocal.isValid());
            
            return !!startDefined && !!endDefined;
        });
        
        this.initValidation();
        datePickerBindingHandler.install();
    }
    
    private initValidation() {
        this.startDateLocal.extend({
            required: {
                onlyIf: () => !this.useMinStartDate()
            },
            validation: [
                {
                    validator: () => {
                        if (this.useMinStartDate()) {
                            return true;
                        }
                        return this.startDateLocal().isValid();
                    },
                    message: "Please enter a valid date"
                }
            ]
        });
        
        this.endDateLocal.extend({
            required: {
                onlyIf: () => !this.useMaxEndDate()
            },
            validation: [
                {
                    validator: () => {
                        if (this.useMaxEndDate()) {
                            return true;
                        }
                        return this.endDateLocal().isValid();
                    },
                    message: "Please enter a valid date"
                },
                {
                    validator: () => {
                        if (this.useMaxEndDate() || this.useMinStartDate()) {
                            return true;
                        }
                        
                        if (!this.startDateLocal() || !this.startDateLocal().isValid()) {
                            return true;
                        }
                        
                        // at this point both start/end are defined and valid, we can compare
                        return this.endDateLocal().diff(this.startDateLocal()) >= 0;
                    },
                    message: "End Date must be greater than (or equal to) Start Date"
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            startDate: this.startDateLocal,
            endDate: this.endDateLocal
        });
    }
    
    private createDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.DeleteOperation[] {
        switch (this.criteria.mode) {
            case "all":
                return [
                    {
                        From: null,
                        To: null
                    }
                ];
            case "selection":
                return this.criteria.selection.map(x => ({
                    From: x.Timestamp,
                    To: x.Timestamp
                }));
            case "range":
                return [{
                    From: this.startDateUTC(),
                    To: this.endDateUTC(),
                }];
        }
    }
    
    deleteItems() {
        const valid = this.criteria.mode === "range" ? this.isValid(this.validationGroup) : true;

        if (valid) {
            const dto = this.createDto();
            
            this.spinners.delete(true);
            
            new deleteTimeSeriesCommand(this.documentId, this.timeSeriesName, dto, this.db)
                .execute()
                .done(() => {
                    const postDelete: postTimeSeriesDeleteAction = this.criteria.mode === "all" ? "changeTimeSeries" : "reloadCurrent";
                    messagePublisher.reportSuccess("Deleted time series values");
                    dialog.close(this, postDelete);
                })
                .always(() => this.spinners.delete(false));
        }
    }

    cancel() {
        dialog.close(this, "doNothing" as postTimeSeriesDeleteAction);
    }

    deactivate() {
        super.deactivate(null);
    }
}

export = deleteTimeSeries;
