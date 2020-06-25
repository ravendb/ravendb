import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import dialog = require("plugins/dialog");
import deleteTimeSeriesCommand = require("commands/database/documents/timeSeries/deleteTimeSeriesCommand");
import messagePublisher = require("common/messagePublisher");
import datePickerBindingHandler = require("common/bindingHelpers/datePickerBindingHandler");
import generalUtils = require("common/generalUtils");

class deleteTimeSeries extends dialogViewModelBase {

    static readonly minDate = "0001-01-01T00:00:00.000Z";
    static readonly maxDate = "9999-12-31T23:59:59.999Z";
    
    spinners = {
        delete: ko.observable<boolean>(false)
    };

    datePickerOptions = {
        format: "YYYY-MM-DD HH:mm:ss.SSS",
        sideBySide: true
    };
    
    useMinStartDate = ko.observable<boolean>(false);
    startDate = ko.observable<moment.Moment>();
    
    useMaxEndDate = ko.observable<boolean>(false);
    endDate = ko.observable<moment.Moment>();
    
    startDateToUse: KnockoutComputed<string>;
    endDateToUse: KnockoutComputed<string>;
    showWarning: KnockoutComputed<boolean>;
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(private timeSeriesName: string, private documentId: string, private db: database, private criteria: timeSeriesDeleteCriteria) {
        super();
        criteria.selection = criteria.selection || [];
        
        this.startDateToUse = ko.pureComputed(() => {
            return this.useMinStartDate() ? deleteTimeSeries.minDate : this.startDate().utc().format(generalUtils.utcFullDateFormat);
        });
        
        this.endDateToUse = ko.pureComputed(() => {
            return this.useMaxEndDate() ? deleteTimeSeries.maxDate : this.endDate().utc().format(generalUtils.utcFullDateFormat);
        });
        
        this.showWarning = ko.pureComputed(() => {
            const startDefined = this.useMinStartDate() || this.startDate();
            const endDefined = this.useMaxEndDate() || this.endDate();
            
            return !!startDefined && !!endDefined;
        });
        
        this.initValidation();
        datePickerBindingHandler.install();
    }
    
    private initValidation() {
        this.startDate.extend({
            required: {
                onlyIf: () => !this.useMinStartDate()
            },
            validation: [
                {
                    validator: () => {
                        if (this.useMinStartDate()) {
                            return true;
                        }
                        return this.startDate().isValid();
                    },
                    message: "Please enter a valid date"
                }
            ]
        });
        
        this.endDate.extend({
            required: {
                onlyIf: () => !this.useMaxEndDate()
            },
            validation: [
                {
                    validator: () => {
                        if (this.useMaxEndDate()) {
                            return true;
                        }
                        return this.endDate().isValid();
                    },
                    message: "Please enter a valid date"
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            startDate: this.startDate,
            endDate: this.endDate
        });
    }
    
    private createDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.RemoveOperation[] {
        switch (this.criteria.mode) {
            case "all":
                return [
                    {
                        From: deleteTimeSeries.minDate,
                        To: deleteTimeSeries.maxDate
                    }
                ];
            case "selection":
                return this.criteria.selection.map(x => ({
                    From: x.Timestamp,
                    To: x.Timestamp
                }));
            case "range":
                return [{
                    From: this.startDateToUse(),
                    To: this.endDateToUse(),
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
