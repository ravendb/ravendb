import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import timeSeriesEntryModel = require("models/database/timeSeries/timeSeriesEntryModel");
import saveTimeSeriesCommand = require("commands/database/documents/timeSeries/saveTimeSeriesCommand");

class editTimeSeriesEntry extends dialogViewModelBase {

    static aggregationColumns = timeSeriesEntryModel.aggregationColumns;
    
    static utcTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    static localTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    
    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    datePickerOptions = {
        format: "YYYY-MM-DD HH:mm:ss.SSS",
        sideBySide: true
    };
    
    model = ko.observable<timeSeriesEntryModel>();
    
    dateFormattedAsUtc: KnockoutComputed<string>;
    dateFormattedAsLocal: KnockoutComputed<string>;
    
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
            ? new timeSeriesEntryModel(timeSeriesName, editDto) 
            : timeSeriesEntryModel.empty(timeSeriesName || undefined);
        
        this.model(model);
        
        this.dateFormattedAsUtc = ko.pureComputed(() => {
            if (model.timestamp()) {
                const date = moment(model.timestamp());
                if (!date.isValid()) {
                    return "Invalid date";
                }
                return date.utc().format(editTimeSeriesEntry.utcTimeFormat) + "Z (UTC)";
            } else {
                return "";
            }
        });
        
        this.dateFormattedAsLocal = ko.pureComputed(() => {
            const date = moment(model.timestamp());
            return date.local().format(editTimeSeriesEntry.localTimeFormat) + " (local)"
        });
    }

    compositionComplete() {
        super.compositionComplete();
        this.setupDisableReasons(".edit-time-series-entry");
    }
    
    getColumnName(idx: number) {
        if (this.columnNames.length && idx < this.columnNames.length) {
            return this.columnNames[idx];
        } 
        
        const aggregationsCount = editTimeSeriesEntry.aggregationColumns.length;
        
        if (this.model().isRollupEntry()) {
            return editTimeSeriesEntry.aggregationColumns[idx % aggregationsCount] + " (Value #" + Math.floor(idx / aggregationsCount) + ")"
        }
        
        // don't display any name!
        return null;
    }
    
    extractValueName(idx: number) {
        const columnName = this.getColumnName(idx);
        const first = columnName.indexOf('(') + 1;
        const last = columnName.lastIndexOf(')');
        return columnName.substring(first, last);
    }
    
    save() {
        const valid = this.model().isRollupEntry() ?
            !this.model().rollupValues().filter(x => !this.isValid(x.validationGroup)).length :
            !this.model().values().filter(x => !this.isValid(x.validationGroup)).length;
        
        if (!this.isValid(this.model().validationGroup) || !valid) {
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
