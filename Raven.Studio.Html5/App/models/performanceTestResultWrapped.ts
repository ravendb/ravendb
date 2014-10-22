class performanceTestResultWrapped {

    totalRead: KnockoutObservable<string>;
    totalWrite: KnockoutObservable<string>;

    avgRead: KnockoutObservable<string>;
    avgWrite: KnockoutObservable<string>;

    hasReads: KnockoutObservable<boolean>;
    hasWrites: KnockoutObservable<boolean>;
    testTime: KnockoutObservable<string>;

    readLatencyMin: KnockoutComputed<string>;
    readLatencyMax: KnockoutComputed<string>;
    readLatencyMean: KnockoutComputed<string>;
    readLatencyStdev: KnockoutComputed<string>;

    writeLatencyMin: KnockoutComputed<string>;
    writeLatencyMax: KnockoutComputed<string>;
    writeLatencyMean: KnockoutComputed<string>;
    writeLatencyStdev: KnockoutComputed<string>;

    readPercentiles: KnockoutComputed<any[]>;
    writePercentiles: KnockoutComputed<any[]>;

    constructor(dto: diskPerformanceResultWrappedDto) {
        this.totalRead = ko.computed(() => (dto.Result.TotalRead / 1024 / 1024).toLocaleString() + "MB");
        this.totalWrite = ko.computed(() => (dto.Result.TotalWrite / 1024 / 1024).toLocaleString() + "MB");
        this.hasReads = ko.computed(() => dto.Request.OperationType == "Read" || dto.Request.OperationType == "Mix");
        this.hasWrites = ko.computed(() => dto.Request.OperationType == "Write" || dto.Request.OperationType == "Mix");
        this.testTime = ko.computed(() => dto.Request.TimeToRunInSeconds + "s");
        this.avgRead = ko.computed(() => (dto.Result.TotalRead / 1024 / 1024 / dto.Request.TimeToRunInSeconds).toLocaleString() + "MB/s");
        this.avgWrite = ko.computed(() => (dto.Result.TotalWrite / 1024 / 1024 / dto.Request.TimeToRunInSeconds).toLocaleString() + "MB/s");

        this.readLatencyMin = ko.computed(() => dto.Result.ReadLatency.Min.toFixed(3) + "ms");
        this.readLatencyMax = ko.computed(() => dto.Result.ReadLatency.Max.toFixed(3) + "ms");
        this.readLatencyMean = ko.computed(() => dto.Result.ReadLatency.Mean.toFixed(3) + "ms");
        this.readLatencyStdev = ko.computed(() => dto.Result.ReadLatency.Stdev.toFixed(3) + "ms");

        this.writeLatencyMin = ko.computed(() => dto.Result.WriteLatency.Min.toFixed(3) + "ms");
        this.writeLatencyMax = ko.computed(() => dto.Result.WriteLatency.Max.toFixed(3) + "ms");
        this.writeLatencyMean = ko.computed(() => dto.Result.WriteLatency.Mean.toFixed(3) + "ms");
        this.writeLatencyStdev = ko.computed(() => dto.Result.WriteLatency.Stdev.toFixed(3) + "ms");

        this.readPercentiles = ko.computed(() => $.map(dto.Result.ReadLatency.Percentiles, (value, key) => {
            return {
                key: key,
                value: value
            }
        }));
        this.writePercentiles = ko.computed(() => $.map(dto.Result.WriteLatency.Percentiles, (value, key) => {
            return {
                key: key,
                value: value
            }
        }));
    }

}

export = performanceTestResultWrapped;