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

    valuesNames = ko.observableArray<string>([]);
    
    constructor(private documentId: string,
                private db: database,
                private timeSeriesName: string,
                private valuesNamesProvider: (timeseriesName: string) => string[],
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

        if (!!this.timeSeriesName) {
            this.getValuesNames();
        }
        
        this.model().name.subscribe(() => this.getValuesNames());
    }
    
    private getValuesNames() {
        const valuesNames = this.valuesNamesProvider(this.model().name());
        this.valuesNames(valuesNames);
    }

    compositionComplete() {
        super.compositionComplete();
        this.setupDisableReasons(".edit-time-series-entry");
    }
    
    getValueName(idx: number) {
        return ko.pureComputed(() => {
            if (this.valuesNames().length) {
                // for an existing timeseries
                return this.valuesNames()[idx];
            } else {
                // for a new timeseries
                const possibleValuesCount = timeSeriesEntryModel.numberOfPossibleValues;
                const possibleValuesNames = _.range(0, possibleValuesCount).map(idx => "Value #" + idx);
                return possibleValuesNames[idx];
            }
        });
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
