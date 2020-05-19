/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");

class trafficItem {
    sizeFormatter = generalUtils.formatBytesToSize;

    database = ko.observable<string>();
    requestsPerSecond = ko.observable<number>();
    docsWritesPerSeconds = ko.observable<number>();
    attachmentsWritesPerSecond = ko.observable<number>();
    countersWritesPerSecond = ko.observable<number>();
    timeSeriesWritesPerSecond = ko.observable<number>();
    docsWriteBytesPerSeconds = ko.observable<number>();
    attachmentsWriteBytesPerSecond = ko.observable<number>();
    countersWriteBytesPerSecond = ko.observable<number>();
    timeSeriesWriteBytesPerSecond = ko.observable<number>();
    writesPerSecond = ko.observable<number>();
    averageDuration = ko.observable<number>();
    dataWritesPerSecond = ko.observable<number>();
    
    showOnGraph = ko.observable<boolean>();

    constructor(dto: Raven.Server.Dashboard.TrafficWatchItem) {
        this.update(dto);
    }
    
    update(dto: Raven.Server.Dashboard.TrafficWatchItem) {
        this.database(dto.Database);
        this.requestsPerSecond(dto.RequestsPerSecond);
        this.docsWritesPerSeconds(dto.DocumentWritesPerSecond);
        this.attachmentsWritesPerSecond(dto.AttachmentWritesPerSecond);
        this.countersWritesPerSecond(dto.CounterWritesPerSecond);
        this.timeSeriesWritesPerSecond(dto.TimeSeriesWritesPerSecond);
        this.docsWriteBytesPerSeconds(dto.DocumentsWriteBytesPerSecond);
        this.attachmentsWriteBytesPerSecond(dto.AttachmentsWriteBytesPerSecond);
        this.countersWriteBytesPerSecond(dto.CountersWriteBytesPerSecond);
        this.timeSeriesWriteBytesPerSecond(dto.TimeSeriesWriteBytesPerSecond);
        this.writesPerSecond(dto.DocumentWritesPerSecond + dto.AttachmentWritesPerSecond + dto.CounterWritesPerSecond + dto.TimeSeriesWritesPerSecond);
        this.dataWritesPerSecond(dto.DocumentsWriteBytesPerSecond + dto.AttachmentsWriteBytesPerSecond + dto.CountersWriteBytesPerSecond + dto.TimeSeriesWriteBytesPerSecond);
        this.averageDuration(dto.AverageRequestDuration);
    }
}

export = trafficItem;
