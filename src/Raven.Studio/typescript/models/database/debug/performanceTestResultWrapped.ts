/// <reference path="../../../../typings/tsd.d.ts"/>

import d3 = require("d3");

class performanceTestResultWrapped {

    private formatter = d3.format(",.2f");

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
        this.totalRead = ko.computed(() => this.formatter(dto.Result.TotalRead / 1024 / 1024) + " MB");
        this.totalWrite = ko.computed(() => this.formatter(dto.Result.TotalWrite / 1024 / 1024) + " MB");
        this.hasReads = ko.computed(() => dto.Request.TestType == "generic" && (dto.Request.OperationType == "Read" || dto.Request.OperationType == "Mix"));
        this.hasWrites = ko.computed(() => dto.Request.TestType == "batch" || dto.Request.OperationType == "Write" || dto.Request.OperationType == "Mix");
        this.testTime = ko.computed(() => this.formatter(dto.Result.TotalTimeMs / 1000) + "s");
        this.avgRead = ko.computed(() => this.formatter(dto.Result.TotalRead / 1024 / 1024 / (dto.Result.TotalTimeMs / 1000)) + "MB/s");
        this.avgWrite = ko.computed(() => this.formatter(dto.Result.TotalWrite / 1024 / 1024 / (dto.Result.TotalTimeMs / 1000)) + "MB/s");

        this.readLatencyMin = ko.computed(() => this.formatter(dto.Result.ReadLatency.Min) + "ms");
        this.readLatencyMax = ko.computed(() => this.formatter(dto.Result.ReadLatency.Max) + "ms");
        this.readLatencyMean = ko.computed(() => this.formatter(dto.Result.ReadLatency.Mean) + "ms");
        this.readLatencyStdev = ko.computed(() => this.formatter(dto.Result.ReadLatency.Stdev) + "ms");

        this.writeLatencyMin = ko.computed(() => this.formatter(dto.Result.WriteLatency.Min) + "ms");
        this.writeLatencyMax = ko.computed(() => this.formatter(dto.Result.WriteLatency.Max) + "ms");
        this.writeLatencyMean = ko.computed(() => this.formatter(dto.Result.WriteLatency.Mean) + "ms");
        this.writeLatencyStdev = ko.computed(() => this.formatter(dto.Result.WriteLatency.Stdev) + "ms");

        this.readPercentiles = ko.computed(() => $.map(dto.Result.ReadLatency.Percentiles, (value, key) => {
            return {
                key: key,
                value: this.formatter(value)
            }
        }));
        this.writePercentiles = ko.computed(() => $.map(dto.Result.WriteLatency.Percentiles, (value, key) => {
            return {
                key: key,
                value: this.formatter(value)
            }
        }));
    }

}

export = performanceTestResultWrapped;
