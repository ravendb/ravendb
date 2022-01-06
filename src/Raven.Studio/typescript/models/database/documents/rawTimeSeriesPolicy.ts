import timeSeriesPolicy = require("models/database/documents/timeSeriesPolicy");

class rawTimeSeriesPolicy extends timeSeriesPolicy {

    constructor(dto: Raven.Client.Documents.Operations.TimeSeries.RawTimeSeriesPolicy) {
        super(dto);
        this.name("rawpolicy");
        this.hasAggregation = false;
    }

    static emptyPolicy: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesPolicy = {
        AggregationTime: null,
        RetentionTime: null,
        Name: null
    }

    static empty() {
        return new rawTimeSeriesPolicy(rawTimeSeriesPolicy.emptyPolicy);
    }
}

export = rawTimeSeriesPolicy;
