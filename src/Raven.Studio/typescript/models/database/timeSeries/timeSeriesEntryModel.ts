import timeSeriesValue = require("models/database/timeSeries/timeSeriesValue");
import generalUtils = require("common/generalUtils");

class rollupDataModel {
    first: timeSeriesValue;
    last: timeSeriesValue;
    min: timeSeriesValue;
    max: timeSeriesValue;
    sum: timeSeriesValue;
    count = ko.observable<number>();

    validationGroup: KnockoutValidationGroup;
    
    constructor(...values: number[]) {
        this.first = new timeSeriesValue(values[0]);
        this.last = new timeSeriesValue(values[1]);
        this.min = new timeSeriesValue(values[2]);
        this.max = new timeSeriesValue(values[3]);
        this.sum = new timeSeriesValue(values[4]);
        this.count(values[5] || 0);

        this.count.extend({
            required: true,
            digit: true
        });

        this.validationGroup = ko.validatedObservable({
            first: this.first.value,
            last: this.last.value,
            min: this.min.value,
            max: this.max.value,
            sum: this.sum.value,
            count: this.count
        });
    }
    
    getValues(): number[] {
        return [this.first.value(),
                this.last.value(),
                this.min.value(),
                this.max.value(),
                this.sum.value(),
                this.count()];
    }
}

class timeSeriesEntryModel {
    
    static aggregationColumns = ["First", "Last", "Min", "Max", "Sum", "Count"];
    static readonly numberOfPossibleValues = 32;
    static readonly numberOfPossibleRollupValues = 5;

    name = ko.observable<string>();
    tag = ko.observable<string>();
    timestamp = ko.observable<moment.Moment>();
    isRollupEntry = ko.observable<boolean>();
    
    values = ko.observableArray<timeSeriesValue>([]);
    rollupValues = ko.observableArray<rollupDataModel>([]);

    maxNumberOfValuesReachedWarning: KnockoutComputed<string>;

    canEditName: boolean;
    validationGroup: KnockoutValidationGroup;
    
    constructor(timeSeriesName: string, dto: Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry) {
        this.name(timeSeriesName);
        this.tag(dto.Tag);
        this.timestamp(dto.Timestamp ? moment.utc(dto.Timestamp) : null);
        this.isRollupEntry(dto.IsRollup);
        
        if (dto.IsRollup) {
            const values = dto.Values;
            
            // Check if rollup data values are Not a multiple of the 6 pre-defined aggregationColumns columns
            // Add null values for the template if needed.
            const currentLength = values.length;
            const remainder = currentLength % timeSeriesEntryModel.aggregationColumns.length;
            const numberOfMissingValues = remainder ? timeSeriesEntryModel.aggregationColumns.length - remainder : 0;
            
            for (let i = 0; i < numberOfMissingValues; i++) {
                values[currentLength + i] = null;
            }
            
            for (let i = 0; i < values.length; i += timeSeriesEntryModel.aggregationColumns.length) {
                const rollup = new rollupDataModel(values[i], values[i+1], values[i+2], values[i+3], values[i+4], values[i+5]);
                this.rollupValues().push(rollup);
            }
        } else {
            this.values(dto.Values.map(x => new timeSeriesValue(x)));
        }
        
        this.canEditName = !timeSeriesName;
        this.initValidation();
        
        this.maxNumberOfValuesReachedWarning = ko.pureComputed(() => {
            if (this.isRollupEntry && this.rollupValues().length === timeSeriesEntryModel.numberOfPossibleRollupValues) {
                return `The maximum number of possible rollup values (${timeSeriesEntryModel.numberOfPossibleRollupValues}) has been reached.`;
            }
            
            if (!this.isRollupEntry() && this.values().length === timeSeriesEntryModel.numberOfPossibleValues) {
                return `The maximum number of possible values (${timeSeriesEntryModel.numberOfPossibleValues}) has been reached.`;
            }

            return "";
        });
    }

    addValue() {
        if (this.isRollupEntry()) {
            const newRollupData = new rollupDataModel();
            this.rollupValues.push(newRollupData);
        } else {
            const newValue = new timeSeriesValue();
            this.values.push(newValue);
        }
    }
    
    removeValue(value: timeSeriesValue) {
        this.values.remove(value);
    }

    removeRollupData(rollup: rollupDataModel) {
        this.rollupValues.remove(rollup);
    }
    
    private initValidation() {
        this.name.extend({
            required: true,
            validation: [
                {
                    validator: () => this.isRollupEntry() || !this.canEditName || !this.name().includes("@"),
                    message: "A Time Series name cannot contain '@'. This character is reserved for Time Series Rollups."
                }
            ]
        });
        
        this.timestamp.extend({
            required: true,
            validation: [
                {
                    validator: () => this.timestamp().isValid(),
                    message: "Please enter a valid date"
                }
            ]
        });

        this.tag.extend({
            maxLength: 255
        });
        
        this.values.extend({
            validation: [
                {
                    validator: () => this.isRollupEntry() || this.values().length > 0,
                    message: "At least one value is required"
                }
            ]
        });
        
        this.rollupValues.extend({
            validation: [
                {
                    validator: () => !this.isRollupEntry() || this.rollupValues().length > 0,
                    message: "At least one rollup value is required"
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            name: this.name,
            tag: this.tag,
            timestamp: this.timestamp,
            values: this.values,
            rollupValues: this.rollupValues
        });
    }
    
    private flattenRollupValues() {
        return this.rollupValues().reduce((result: number[], next: rollupDataModel) => {
            result.push(...next.getValues());
            return result;
        }, []);
    }
    
    public toDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.AppendOperation {
        return {
            Tag: this.tag(),
            Timestamp: this.timestamp().utc().format(generalUtils.utcFullDateFormat),
            Values: this.isRollupEntry() ? this.flattenRollupValues() : this.values().map(x => x.value())
        }
    }
    
    static empty(timeSeriesName: string) {
        return new timeSeriesEntryModel(timeSeriesName, {
            Timestamp: null,
            Tag: null,
            Values: [],
            IsRollup: timeSeriesName && timeSeriesName.includes("@")
        });
    }
}

export = timeSeriesEntryModel;
