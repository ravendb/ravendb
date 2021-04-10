import generalUtils = require("common/generalUtils");
import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");

class serverTraffic extends historyAwareNodeStats<Raven.Server.Dashboard.Cluster.Notifications.TrafficWatchPayload> {

    attachmentsWriteBytesPerSecond = this.conditionalDataExtractor(x => generalUtils.formatBytesToSize(x.AttachmentsWriteBytesPerSecond) + "/s", { customNoData: "n/a" });
    attachmentWritesPerSecond = this.conditionalDataExtractor(x => x.AttachmentWritesPerSecond.toLocaleString() + "/s", { customNoData: "n/a" });
    countersWriteBytesPerSecond = this.conditionalDataExtractor(x => generalUtils.formatBytesToSize(x.CountersWriteBytesPerSecond) + "/s", { customNoData: "n/a" });
    counterWritesPerSecond = this.conditionalDataExtractor(x => x.CounterWritesPerSecond.toLocaleString() + "/s", { customNoData: "n/a" });
    documentsWriteBytesPerSecond = this.conditionalDataExtractor(x => generalUtils.formatBytesToSize(x.DocumentsWriteBytesPerSecond) + "/s", { customNoData: "n/a" });
    documentWritesPerSecond = this.conditionalDataExtractor(x => x.DocumentWritesPerSecond.toLocaleString() + "/s", { customNoData: "n/a" });
    requestsPerSecond = this.dataExtractor(x => x.RequestsPerSecond);
    timeSeriesWriteBytesPerSecond = this.conditionalDataExtractor(x => generalUtils.formatBytesToSize(x.TimeSeriesWriteBytesPerSecond) + "/s", { customNoData: "n/a" });
    timeSeriesWritesPerSecond = this.conditionalDataExtractor(x => x.TimeSeriesWritesPerSecond.toLocaleString() + "/s", { customNoData: "n/a" });
    
    totalWritesPerSecond = this.dataExtractor(x => x.DocumentWritesPerSecond
        + x.AttachmentWritesPerSecond
        + x.CounterWritesPerSecond
        + x.TimeSeriesWritesPerSecond);
    
    totalWritesPerSecondSize = this.conditionalDataExtractor(() => generalUtils.formatBytesToSize(this.totalWritesPerSecond(), 2, true)[0]);
    totalWritesPerSecondUnit = this.conditionalDataExtractor(() => generalUtils.formatBytesToSize(this.totalWritesPerSecond(), 2, true)[1], { customNoData: "-" });
    
    totalWriteBytesPerSecond = this.dataExtractor(x => x.DocumentsWriteBytesPerSecond
        + x.AttachmentsWriteBytesPerSecond
        + x.CountersWriteBytesPerSecond
        + x.TimeSeriesWriteBytesPerSecond);

    totalWriteBytesPerSecondSize = this.conditionalDataExtractor(() => generalUtils.formatBytesToSize(this.totalWriteBytesPerSecond(), 2, true)[0]);
    totalWriteBytesPerSecondUnit = this.conditionalDataExtractor(() => generalUtils.formatBytesToSize(this.totalWriteBytesPerSecond(), 2, true)[1], { customNoData: "-" });
    
    requestsFormatted: KnockoutComputed<string>;

    constructor(tag: string) {
        super(tag);
        
        this.requestsFormatted = ko.pureComputed(() => {
            const noData = this.noDataText();
            if (noData) {
                return noData;
            }
            return this.requestsPerSecond().toLocaleString();
        });
    }
}

export = serverTraffic;
