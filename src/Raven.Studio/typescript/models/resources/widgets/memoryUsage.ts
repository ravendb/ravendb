import generalUtils = require("common/generalUtils");

import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");

class memoryUsage extends historyAwareNodeStats<Raven.Server.Dashboard.Cluster.Notifications.MemoryUsagePayload> {

    availableMemory = this.dataExtractor(x => x.AvailableMemory);
    lowMemorySeverity = this.dataExtractor(x => x.LowMemorySeverity);
    physicalMemory = this.dataExtractor(x => x.PhysicalMemory);
    workingSet = this.dataExtractor(x => x.WorkingSet);
    managedAllocations = this.dataExtractor(x => x.ManagedAllocations);
    dirtyMemory = this.dataExtractor(x => x.DirtyMemory);
    encryptionBuffersInUse = this.dataExtractor(x => x.EncryptionBuffersInUse);
    encryptionBuffersPool = this.dataExtractor(x => x.EncryptionBuffersPool);
    memoryMapped = this.dataExtractor(x => x.MemoryMapped);
    unmanagedAllocations = this.dataExtractor(x => x.UnmanagedAllocations);
    availableMemoryForProcessing = this.dataExtractor(x => x.AvailableMemoryForProcessing);
    systemCommitLimit = this.dataExtractor(x => x.SystemCommitLimit);
    swap = this.dataExtractor(x => x.PhysicalMemory != null && x.SystemCommitLimit != null ? x.SystemCommitLimit - x.PhysicalMemory : undefined);
    
    workingSetFormatted: KnockoutComputed<[string, string]>;
    machineMemoryUsage: KnockoutComputed<string>;
    machineMemoryUsagePercentage: KnockoutComputed<string>;
    lowMemoryTitle: KnockoutComputed<string>;
    showEncryptionBuffers: KnockoutComputed<boolean>;

    sizeFormatter = generalUtils.formatBytesToSize;

    constructor(tag: string) {
        super(tag);

        this.showEncryptionBuffers = ko.pureComputed(() => {
            return location.protocol === "https:";
        });
        
        this.workingSetFormatted = this.valueAndUnitFormatter(this.workingSet);

        this.machineMemoryUsage = this.conditionalDataExtractor(x => {
            const physical = this.physicalMemory();
            const available = this.availableMemory();

            const used = physical - available;
            const usedFormatted = generalUtils.formatBytesToSize(used).split(" ");
            const totalFormatted = generalUtils.formatBytesToSize(physical).split(" ");

            if (usedFormatted[1] === totalFormatted[1]) { // same units - avoid repeating ourselves
                return usedFormatted[0] + " / " + totalFormatted[0] + " " + totalFormatted[1];
            } else {
                return usedFormatted[0] + " " + usedFormatted[1] + " / " + totalFormatted[0] + " " + totalFormatted[1];
            }
        }, { 
            customNoData: "-"
        });

        this.machineMemoryUsagePercentage = this.conditionalDataExtractor(x => {
            const physical = x.PhysicalMemory;
            const available = x.AvailableMemory;

            if (!physical) {
                return "n/a";
            }

            return Math.round(100.0 * (physical - available) / physical) + '%';
        });

        this.lowMemoryTitle = ko.pureComputed(() => {
            const lowMem = this.lowMemorySeverity();
            if (lowMem === "ExtremelyLow") {
                return "Extremely Low Memory Mode";
            } else if (lowMem === "Low") {
                return "Low Memory Mode";
            }

            return null;
        })
    }
    
    valueAndUnitFormatter(value: KnockoutObservable<number>): KnockoutComputed<[string, string]> {
        return ko.pureComputed(() => {
            const noData = this.noDataText();
            if (noData) {
                return [noData, "-"];
            }

            const formatted = generalUtils.formatBytesToSize(value());
            return formatted.split(" ", 2) as [string, string];
        });
    }
}

export = memoryUsage;
