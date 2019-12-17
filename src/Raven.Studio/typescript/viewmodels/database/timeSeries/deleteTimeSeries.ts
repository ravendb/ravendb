import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import dialog = require("plugins/dialog");
import deleteTimeSeriesCommand = require("commands/database/documents/timeSeries/deleteTimeSeriesCommand");
import messagePublisher = require("common/messagePublisher");

class deleteTimeSeries extends dialogViewModelBase {

    static readonly minDate = "0001-01-01T00:00:00.0000000Z";
    static readonly maxDate = "9999-12-31T23:59:59.9999999Z";
    
    spinners = {
        delete: ko.observable<boolean>(false)
    };
    
    useStartDate = ko.observable<boolean>(false);
    startDate = ko.observable<string>();
    
    useEndDate = ko.observable<boolean>(false);
    endDate = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(private timeSeriesName: string, private documentId: string, private db: database, private criteria: timeSeriesDeleteCriteria) {
        super();
        criteria.selection = criteria.selection || [];
        
        this.initValidation();
    }
    
    private initValidation() {
        this.startDate.extend({
            required: {
                onlyIf: () => this.useStartDate()
            }
        });
        
        this.endDate.extend({
            required: {
                onlyIf: () => this.useEndDate()
            }
        });
        
        this.validationGroup = ko.validatedObservable({
            startDate: this.startDate,
            endDate: this.endDate
        });
    }
    
    private createDto(): Raven.Client.Documents.Operations.TimeSeries.RemoveTimeSeriesOperation[] {
        switch (this.criteria.mode) {
            case "all":
                return [
                    {
                        Name: this.timeSeriesName,
                        From: deleteTimeSeries.minDate,
                        To: deleteTimeSeries.maxDate
                    }
                ];
            case "selection":
                return this.criteria.selection.map(x => ({
                    Name: this.timeSeriesName,
                    From: x.Timestamp,
                    To: x.Timestamp
                }));
            case "range":
                return [{
                    From: this.startDate(),
                    To: this.endDate(),
                    Name: this.timeSeriesName
                }];
        }
    }
    
    deleteItems() {
        const valid = this.criteria.mode === "range" ? this.isValid(this.validationGroup) : true;

        if (valid) {
            const dto = this.createDto();
            
            this.spinners.delete(true);
            
            new deleteTimeSeriesCommand(this.documentId, dto, this.db)
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
