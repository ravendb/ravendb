import generalUtils = require("common/generalUtils");

class memoryUsage {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);

    hasData = ko.observable<boolean>(false);

    availableMemory = ko.observable<number>();
    lowMemorySeverity = ko.observable<Sparrow.LowMemory.LowMemorySeverity>();
    physicalMemory = ko.observable<number>();
    workingSet = ko.observable<number>();
    managedAllocations = ko.observable<number>();
    dirtyMemory = ko.observable<number>();
    encryptionBuffersInUse = ko.observable<number>(); //TODO: consider hiding if encryption not used on server?
    encryptionBuffersPool = ko.observable<number>();
    memoryMapped = ko.observable<number>();
    unmanagedAllocations = ko.observable<number>();
    availableMemoryForProcessing = ko.observable<number>();
    systemCommitLimit = ko.observable<number>();

    workingSetFormatted: KnockoutComputed<[string, string]>;
    machineMemoryUsage: KnockoutComputed<string>;
    machineMemoryUsagePercentage: KnockoutComputed<string>;
    lowMemoryTitle: KnockoutComputed<string>;

    sizeFormatter = generalUtils.formatBytesToSize;

    constructor(tag: string) {
        this.tag = tag;

        this.workingSetFormatted = this.valueAndUnitFormatter(this.workingSet);

        this.machineMemoryUsage = ko.pureComputed(() => {
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
        })

        this.machineMemoryUsagePercentage = ko.pureComputed(() => {
            const physical = this.physicalMemory();
            const available = this.availableMemory();

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

    update(data: Raven.Server.Dashboard.Cluster.Notifications.MemoryUsagePayload) {
        this.hasData(true);
        this.availableMemory(data.AvailableMemory);
        this.lowMemorySeverity(data.LowMemorySeverity);
        this.physicalMemory(data.PhysicalMemory);
        this.workingSet(data.WorkingSet);

        this.managedAllocations(data.ManagedAllocations);
        this.dirtyMemory(data.DirtyMemory);
        this.encryptionBuffersInUse(data.EncryptionBuffersInUse);
        this.encryptionBuffersPool(data.EncryptionBuffersPool);
        this.memoryMapped(data.MemoryMapped);
        this.unmanagedAllocations(data.UnmanagedAllocations);
        this.availableMemoryForProcessing(data.AvailableMemoryForProcessing);
        this.systemCommitLimit(data.SystemCommitLimit);
    }

    valueAndUnitFormatter(value: KnockoutObservable<number>): KnockoutComputed<[string, string]> {
        return ko.pureComputed(() => {
            if (this.disconnected() || !this.hasData()) {
                return ["Connecting...", "-"];
            }

            const formatted = generalUtils.formatBytesToSize(value());
            return formatted.split(" ", 2) as [string, string];
        });
    }
}

export = memoryUsage;
