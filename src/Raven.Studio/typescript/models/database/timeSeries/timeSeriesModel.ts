import timeSeriesValue = require("models/database/timeSeries/timeSeriesValue");
import generalUtils = require("common/generalUtils");

class timeSeriesModel {
    
    static aggregationColumns = ["First", "Last", "Min", "Max", "Sum", "Count"];
    
    constructor(name: string, dto: Raven.Client.Documents.Session.TimeSeries.TimeSeriesEntry) {
        this.name(name);
        this.tag(dto.Tag);
        this.timestamp(dto.Timestamp ? moment.utc(dto.Timestamp) : null);
        this.values(dto.Values.map(x => new timeSeriesValue(x)));
        
        this.canEditName = !name;
        this.initValidation();
    }
    
    name = ko.observable<string>();
    tag = ko.observable<string>();
    timestamp = ko.observable<moment.Moment>();
    values = ko.observableArray<timeSeriesValue>([]);
    
    canEditName: boolean;
    validationGroup: KnockoutValidationGroup;

    addValue() {
        const newValue = new timeSeriesValue(0);
        this.values.push(newValue);
    }
    
    removeValue(value: timeSeriesValue) {
        this.values.remove(value);
    }
    
    private initValidation() {
        this.name.extend({
            required: true,
            validation: [
                {
                    validator: () =>  !this.canEditName || !this.name().includes("@"),
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

        this.values.extend({
            validation: [
                {
                    validator: () => this.values().length > 0,
                    message: "At least one value is required"
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            name: this.name,
            tag: this.tag,
            timestamp: this.timestamp,
            values: this.values
        });
    }
    
    public toDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.AppendOperation {
        return {
            Tag: this.tag(),
            Timestamp: this.timestamp().utc().format(generalUtils.utcFullDateFormat),
            Values: this.values().map(x => x.value())
        }
    }
    
    static empty(name: string) {
        return new timeSeriesModel(name, {
            Timestamp: null,
            Tag: null,
            Values: [],
            IsRollup: false
        });
    }
}

export = timeSeriesModel;
