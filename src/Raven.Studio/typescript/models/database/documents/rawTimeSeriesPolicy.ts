import timeSeriesPolicy = require("models/database/documents/timeSeriesPolicy");

class rawTimeSeriesPolicy extends timeSeriesPolicy {

    constructor(dto: Raven.Client.Documents.Operations.TimeSeries.RawTimeSeriesPolicy) {
        super(dto);
        this.name("rawpolicy");
        this.hasAggregation = false;
    }
    
    static empty() {
        return new rawTimeSeriesPolicy({
            AggregationTime: null,
            RetentionTime: {
                Value: 0,
                Unit: "Month"
            },
            Name: null
        });
    }
}

export = rawTimeSeriesPolicy;
