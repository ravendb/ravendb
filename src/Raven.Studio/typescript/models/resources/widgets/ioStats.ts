import IoStatsPayload = Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload;

import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import IoStatsResult = Raven.Client.ServerWide.Operations.IoStatsResult;
import genUtils = require("common/generalUtils");
import storageIoTerms = require("common/storageIoTerms");

class ioStats extends historyAwareNodeStats<IoStatsPayload> {

    // only Operators on servers that send it get the disk status, it stays false otherwise
    hasStorageStatus = ko.observable<boolean>(false);

    storageStatus = this.dataExtractor(x => x.StorageStatus ?? null);

    storageStatusText = ko.pureComputed(() => {
        const status = this.storageStatus();
        return status ? "Disk: " + storageIoTerms.storageStatusLabel[status.Status] : null;
    });

    storageStatusClass = ko.pureComputed(() => {
        const status = this.storageStatus();
        return status ? "label label-" + storageIoTerms.storageStatusColor[status.Status] : "";
    });

    storageStatusNoData = this.conditionalDataExtractor(x => (x.StorageStatus ? null : "n/a"));

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

    onData(data: IoStatsPayload) {
        if (data.StorageStatus) {
            this.hasStorageStatus(true);
        }
        super.onData(data);
    }

    storageStatusDetailsHtml(): string {
        const status = this.storageStatus();
        if (!status) {
            return "";
        }

        const lines: [string, string][] = [
            ["Writes paced in the last hour", Math.round(status.DrainShareOfLastHour * 100) + "%"],
            ["Journal writes", ioStats.formatMs(status.JournalWriteLatencyInMs)],
            ["Data syncs", storageIoTerms.formatSyncCost(status.SyncCostInMs, ioStats.formatMs)],
            ["Mount point", status.MountPoint],
        ];

        const rows = lines
            .map(([name, value]) => `<div>${name}: <strong>${genUtils.escapeHtml(value ?? "-")}</strong></div>`)
            .join("");

        const disksNote =
            status.Disks > 1
                ? `<div class="text-muted margin-top-sm">The busiest of the ${status.Disks} disks this node writes to.</div>`
                : "";

        return `<div class="margin-bottom-sm">${genUtils.escapeHtml(status.Summary)}</div>${rows}${disksNote}`;
    }

    private static formatMs(value: number): string {
        if (value == null) {
            return "-";
        }
        return value.toLocaleString(undefined, { maximumFractionDigits: value < 10 ? 1 : 0 }) + " ms";
    }
    
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
