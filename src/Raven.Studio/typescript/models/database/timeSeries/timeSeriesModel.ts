import timeSeriesValue = require("models/database/timeSeries/timeSeriesValue");

class timeSeriesModel {
    
    constructor(name: string, dto: Raven.Client.Documents.Session.TimeSeriesValue) {
        this.name(name);
        this.tag(dto.Tag);
        this.timestamp(dto.Timestamp);
        this.values(dto.Values.map(x => new timeSeriesValue(x)));
        
        this.initValidation();
    }
    
    name = ko.observable<string>();
    tag = ko.observable<string>();
    timestamp = ko.observable<string>();
    values = ko.observableArray<timeSeriesValue>([]);
    
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
            required: true
        });
        
        this.timestamp.extend({
            required: true
        });

        this.values.extend({
            validation: [
                {
                    validator: () => this.values().length > 0,
                    message: "All least one value is required"
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
    
    public toDto(): Raven.Client.Documents.Operations.TimeSeries.AppendTimeSeriesOperation {
        return {
            Name: this.name(),
            Tag: this.tag(),
            Timestamp: this.timestamp(),
            Values: this.values().map(x => x.value())
        }
    }
    
    static empty(name: string) {
        return new timeSeriesModel(name, {
            Timestamp: null,
            Tag: null,
            Values: []
        });
    }
}

export = timeSeriesModel;
