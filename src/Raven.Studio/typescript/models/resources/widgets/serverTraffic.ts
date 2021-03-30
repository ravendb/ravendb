import generalUtils = require("common/generalUtils");

class serverTraffic {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);

    hasData = ko.observable<boolean>(false);

    attachmentsWriteBytesPerSecond = ko.observable<number>();
    attachmentWritesPerSecond = ko.observable<number>();
    countersWriteBytesPerSecond = ko.observable<number>();
    counterWritesPerSecond = ko.observable<number>();
    documentsWriteBytesPerSecond = ko.observable<number>();
    documentWritesPerSecond = ko.observable<number>();
    requestsPerSecond = ko.observable<number>();
    timeSeriesWriteBytesPerSecond = ko.observable<number>();
    timeSeriesWritesPerSecond = ko.observable<number>();
    
    sizeFormatter = generalUtils.formatBytesToSize;
    
    requestsFormatted: KnockoutComputed<string>;

    constructor(tag: string) {
        this.tag = tag;
        
        this.requestsFormatted = ko.pureComputed(() => {
            const requests = this.requestsPerSecond();
            if (requests != null) {
                return requests.toLocaleString();
            }
            return "n/a";
        })
    }

    update(data: Raven.Server.Dashboard.Cluster.Notifications.TrafficWatchPayload) {
        this.hasData(true);
        this.attachmentsWriteBytesPerSecond(data.AttachmentsWriteBytesPerSecond);
        this.attachmentWritesPerSecond(data.AttachmentWritesPerSecond);
        this.countersWriteBytesPerSecond(data.CountersWriteBytesPerSecond);
        this.counterWritesPerSecond(data.CounterWritesPerSecond);
        this.documentsWriteBytesPerSecond(data.DocumentsWriteBytesPerSecond);
        this.documentWritesPerSecond(data.DocumentWritesPerSecond);
        this.requestsPerSecond(data.RequestsPerSecond);
        this.timeSeriesWriteBytesPerSecond(data.TimeSeriesWriteBytesPerSecond);
        this.timeSeriesWritesPerSecond(data.TimeSeriesWritesPerSecond);
    }
    
    writesFormatter(value: KnockoutObservable<number>): KnockoutComputed<[string, string] | string> {
        return ko.pureComputed(() => {
            if (this.disconnected() || !this.hasData()) {
                return "n/a";
            }
            
            const rawValue = value();
            if (rawValue != null) {
                return rawValue.toLocaleString() + "/s";
            }
            return "n/a";
        });
    }

    dataWrittenFormatter(value: KnockoutObservable<number>, asArray = false): KnockoutComputed<[string, string] | string> {
        return ko.pureComputed(() => {
            if (this.disconnected() || !this.hasData()) {
                return asArray ? ["n/a", ""] : "n/a";
            }

            const rawValue = value();
            if (rawValue != null) {
                const [size, unit] = generalUtils.formatBytesToSize(rawValue).split(" ", 2);
                return asArray ? [size, unit + "/s"] : size + " " + unit + "/s";
            }

            return asArray ? ["n/a", ""] : "n/a";
        });
    }
}

export = serverTraffic;
