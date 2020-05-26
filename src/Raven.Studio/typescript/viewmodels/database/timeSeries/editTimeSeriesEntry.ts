import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import timeSeriesModel = require("models/database/timeSeries/timeSeriesModel");
import saveTimeSeriesCommand = require("commands/database/documents/timeSeries/saveTimeSeriesCommand");

class editTimeSeriesEntry extends dialogViewModelBase {

    static aggregationColumns = timeSeriesModel.aggregationColumns;
    
    static utcTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    static localTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    
    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    datePickerOptions = {
        format: "YYYY-MM-DD HH:mm:ss.SSS",
        sideBySide: true
    };
    
    model = ko.observable<timeSeriesModel>();
    
    dateFormattedAsUtc: KnockoutComputed<string>;
    dateFormattedAsLocal: KnockoutComputed<string>;
    isAggregation: KnockoutComputed<boolean>;
    
    lockSeriesName: boolean;
    lockTimeStamp: boolean;
    
    constructor(private documentId: string, 
                private db: database, 
                private timeSeriesName: string,
                private columnNames: string[],
                private editDto?: Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry) {
        super();
        
        this.lockTimeStamp = !!editDto;
        this.lockSeriesName = !!this.timeSeriesName;
        
        const model = editDto 
            ? new timeSeriesModel(timeSeriesName, editDto) 
            : timeSeriesModel.empty(timeSeriesName || undefined);
        
        this.model(model);
        
        this.dateFormattedAsUtc = ko.pureComputed(() => {
            if (model.timestamp()) {
                const date = moment(model.timestamp());
                return date.utc().format(editTimeSeriesEntry.utcTimeFormat) + "Z (UTC)";    
            } else {
                return "";
            }
        });
        
        this.dateFormattedAsLocal = ko.pureComputed(() => {
            const date = moment(model.timestamp());
            return date.local().format(editTimeSeriesEntry.localTimeFormat) + " (local)"
        });
        
        this.isAggregation = ko.pureComputed(() => {
            const name = this.model().name();
            return name && name.includes("@");
        });
    }
    
    getColumnName(idx: number) {
        if (this.columnNames.length && idx < this.columnNames.length) {
            return this.columnNames[idx];
        } 
        
        const aggregationsCount = editTimeSeriesEntry.aggregationColumns.length;
        
        if (this.isAggregation()) {
            return editTimeSeriesEntry.aggregationColumns[idx % aggregationsCount] + " (Value #" + Math.floor(idx / aggregationsCount) + ")"
        }
        
        // don't display any name!
        return null;
    }
    
    save() {
        if (!this.isValid(this.model().validationGroup)) {
            return false;
        }
        
        this.spinners.save(true);
        
        const dto = this.model().toDto();
        
        new saveTimeSeriesCommand(this.documentId, this.model().name(), dto, this.db)
            .execute()
            .done(() => {
                dialog.close(this, this.model().name());
            })
            .always(() => this.spinners.save(false));
    }

    cancel() {
        dialog.close(this, null);
    }

    deactivate() {
        super.deactivate(null);
    }
}

export = editTimeSeriesEntry;
