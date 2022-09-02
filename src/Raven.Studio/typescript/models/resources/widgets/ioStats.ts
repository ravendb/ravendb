import IoStatsPayload = Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload;

import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import IoStatsResult = Raven.Client.ServerWide.Operations.IoStatsResult;

class ioStats extends historyAwareNodeStats<IoStatsPayload> {
    
    iopsFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.IoReadOperations + i.IoWriteOperations));
    iopsDistribution = this.conditionalDataExtractor(x => {
        const read = ioStats.sumUp(x, i => i.IoReadOperations);
        const write = ioStats.sumUp(x, i => i.IoWriteOperations);
        if (typeof read === "undefined" || typeof write === "undefined") {
            return "n/a";
        }
        return "(read: " + read.toFixed(0) + ", write: " + write.toFixed(0) + ")";
    });
    
    iopsReadFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.IoReadOperations));
    iopsReadSplitInfo = this.conditionalDataExtractor(() => "read/s");
    
    iopsWriteFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.IoWriteOperations));
    iopsWriteSplitInfo = this.conditionalDataExtractor(() => "write/s");

    throughputFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.ReadThroughputInKb + i.WriteThroughputInKb), " KB/s");
    throughputDistribution = this.conditionalDataExtractor(x => {
        const read = ioStats.sumUp(x, i => i.ReadThroughputInKb);
        const write = ioStats.sumUp(x, i => i.WriteThroughputInKb);
        if (typeof read === "undefined" || typeof write === "undefined") {
            return "n/a";
        }
        return "(read: " + read.toFixed(0) + " KB/s, write: " + write.toFixed(0) + " KB/s)";
    });
    
    throughputReadFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.ReadThroughputInKb));
    throughputReadSplitInfo = this.conditionalDataExtractor(() => "read (KB/s)");
    
    throughputWriteFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.WriteThroughputInKb));
    throughputWriteSplitInfo = this.conditionalDataExtractor(() => "write (KB/s)");

    diskQueueFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.QueueLength ?? 0));
    
    private static sumUp(payload: IoStatsPayload, accessor: (item: IoStatsResult) => number): number | undefined {
        if (!payload.Items.length) {
            return undefined;
        }
        return payload.Items.map(accessor)
            .reduce((p, c) => p + c, 0);
    }
    
    private conditionalDataExtractorWithNoDataDetection(accessor: (value: IoStatsPayload) => number | undefined, suffix?: string) {
        return this.conditionalDataExtractor(x => {
            const value = accessor(x);
            if (typeof value === "undefined") {
                return "n/a";
            }
            
            return value.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 }) + (suffix ?? "");
        })
    } 
}


export = ioStats;
