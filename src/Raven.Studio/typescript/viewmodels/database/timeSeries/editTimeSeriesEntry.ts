import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import timeSeriesEntryModel = require("models/database/timeSeries/timeSeriesEntryModel");
import saveTimeSeriesCommand = require("commands/database/documents/timeSeries/saveTimeSeriesCommand");
import popoverUtils = require("common/popoverUtils");
import moment = require("moment");
import { range } from "common/typeUtils";

class editTimeSeriesEntry extends dialogViewModelBase {
    
    view = require("views/database/timeSeries/editTimeSeriesEntry.html");

    static aggregationColumns = timeSeriesEntryModel.aggregationColumns;
    
    static utcTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";
    static localTimeFormat = "YYYY-MM-DD HH:mm:ss.SSS";

    static readonly incrementalTimeSeriesInfo =
        `<ul class="margin-top margin-top-xs no-padding-left margin-left">
            <li><small><strong>Incremental Time Series</strong> allows to increment/decrement values by some delta.<br>
                               The value's total content is the merged content from all nodes.
            </small></li>
            <li><small><strong>Regular Time Series</strong> values that are modified override current value.</small></li>
         </ul>`;
    
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

    showValuesPerNode = ko.observable<boolean>(false);
    
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
            return date.local().format(editTimeSeriesEntry.localTimeFormat) + " (Local)"
        });

        if (this.timeSeriesName) {
            this.getValuesNames();
        }
        
        this.model().name.subscribe(() => this.getValuesNames());
    }
    
    private getValuesNames(): void {
        const valuesNames = this.valuesNamesProvider(this.model().name());
        this.valuesNames(valuesNames);
    }

    compositionComplete() {
        super.compositionComplete();
        this.setupDisableReasons(".edit-time-series-entry");

        popoverUtils.longWithHover($(".create-incremental"),
            {
                content: editTimeSeriesEntry.incrementalTimeSeriesInfo
            });
    }
    
    getValueName(idx: number) {
        return ko.pureComputed(() => {
            if (this.valuesNames().length) {
                // for an existing timeseries
                return this.valuesNames()[idx];
            } else {
                // for a new timeseries
                const possibleValuesCount = timeSeriesEntryModel.numberOfPossibleValues;
                const possibleValuesNames = range(0, possibleValuesCount).map(idx => "Value #" + idx);
                return possibleValuesNames[idx];
            }
        });
    }

    getValueOnNode(nodeIndex: number, valueIndex: number) {
        return ko.pureComputed(() => this.model().nodesDetails()[nodeIndex].nodeValues[valueIndex]);
    }
    
    save() {
        const model = this.model();
        
        const valid = model.isRollupEntry() ?
            !model.rollupValues().filter(x => !this.isValid(x.validationGroup)).length :
            !model.values().filter(x => !this.isValid(x.validationGroup)).length;
        
        if (!this.isValid(model.validationGroup) || !valid) {
            return false;
        }
        
        this.spinners.save(true);

        new saveTimeSeriesCommand(this.documentId, model.name(), model.toDto(), this.db)
            .execute()
            .done(() => {
                dialog.close(this, model.name());
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
