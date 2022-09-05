import IoStatsPayload = Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload;

import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import IoStatsResult = Raven.Client.ServerWide.Operations.IoStatsResult;
import genUtils = require("common/generalUtils");

class ioStats extends historyAwareNodeStats<IoStatsPayload> {
    
    iopsFormatted = this.conditionalDataExtractor(x => {
        const total = ioStats.sumUp(x, i => i.IoReadOperations + i.IoWriteOperations);
        if (typeof total === "undefined") {
            return "n/a";
        }
        return Math.ceil(total).toLocaleString();
    });
    
    iopsReadFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.IoReadOperations));
    iopsReadSplitInfo = this.conditionalDataExtractor(() => "read/s");
    
    iopsWriteFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.IoWriteOperations));
    iopsWriteSplitInfo = this.conditionalDataExtractor(() => "write/s");

    throughputFormatted = this.conditionalDataExtractor(x => {
        const total = ioStats.sumUp(x, i => i.ReadThroughputInKb + i.WriteThroughputInKb);
        if (typeof total === "undefined") {
            return "n/a";
        }
        return genUtils.formatBytesToSize(total * 1024, 2, true)[0];
    });
    
    throughputUnit = this.conditionalDataExtractor(x => {
        const total = ioStats.sumUp(x, i => i.ReadThroughputInKb + i.WriteThroughputInKb);
        if (typeof total === "undefined") {
            return "n/a";
        }
        return genUtils.formatBytesToSize(total * 1024, 2, true)[1] + "/s";
    });
    
    throughputReadFormatted = this.conditionalDataExtractor(x => {
        const total = ioStats.sumUp(x, i => i.ReadThroughputInKb);
        if (typeof total === "undefined") {
            return "n/a";
        }
        return genUtils.formatBytesToSize(total * 1024, 2, true)[0];
    });
    throughputReadSplitInfo = this.conditionalDataExtractor(x => {
        const total = ioStats.sumUp(x, i => i.ReadThroughputInKb);
        if (typeof total === "undefined") {
            return "n/a";
        }
        return "read (" + genUtils.formatBytesToSize(total * 1024, 2, true)[1] + "/s)";
    });
    
    throughputWriteFormatted = this.conditionalDataExtractor(x => {
        const total = ioStats.sumUp(x, i => i.WriteThroughputInKb);
        if (typeof total === "undefined") {
            return "n/a";
        }
        return genUtils.formatBytesToSize(total * 1024, 2, true)[0];
    });
    throughputWriteSplitInfo = this.conditionalDataExtractor(x => {
        const total = ioStats.sumUp(x, i => i.WriteThroughputInKb);
        if (typeof total === "undefined") {
            return "n/a";
        }
        return "write (" + genUtils.formatBytesToSize(total * 1024, 2, true)[1] + "/s)";
    });

    diskQueueFormatted = this.conditionalDataExtractorWithNoDataDetection(x => ioStats.sumUp(x, i => i.QueueLength ?? 0));
    
    private static sumUp(payload: IoStatsPayload, accessor: (item: IoStatsResult) => number): number | undefined {
        if (!payload.Items.length) {
            return undefined;
        }
        return payload.Items.map(accessor)
            .reduce((p, c) => p + c, 0);
    }
    
    private conditionalDataExtractorWithNoDataDetection(accessor: (value: IoStatsPayload) => number | undefined) {
        return this.conditionalDataExtractor(x => {
            const value = accessor(x);
            if (typeof value === "undefined") {
                return "n/a";
            }
            
            return value.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
        });
    } 
}


export = ioStats;
