/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");

class machineResources {
    sizeFormatter = generalUtils.formatBytesToSize;

    machineCpuUsage = ko.observable<number>(0);
    processCpuUsage = ko.observable<number>(0);

    totalMemory = ko.observable<number>(0);
    usedMemory = ko.observable<number>(0);
    availableMemory = ko.observable<number>(0);
    availableMemoryForProcessing = ko.observable<number>(0);
    processMemoryUsage = ko.observable<number>(0);
    systemCommitLimit = ko.observable<number>(0);
    commitedMemory = ko.observable<number>(0);
    isWindows = ko.observable<boolean>();
    isExtremelyLowMemory = ko.observable<boolean>();
    isLowMemory = ko.observable<boolean>();
    lowMemoryThreshold = ko.observable<number>(0);
    commitChargeThreshold = ko.observable<number>(0);

    machineCpuUsageClass: KnockoutComputed<string>;
    processCpuUsageClass: KnockoutComputed<string>;

    totalMemoryTooltip: KnockoutComputed<string>;
    machineMemoryUsageTooltip: KnockoutComputed<string>;
    lowMemoryTooltip: KnockoutComputed<string>;

    constructor(dto: Raven.Server.Dashboard.MachineResources) {
        this.update(dto);
        
        this.machineCpuUsageClass = ko.pureComputed(() => {
            const usage = this.machineCpuUsage();
            return this.getCpuUsageClass(usage);
        });

        this.processCpuUsageClass = ko.pureComputed(() => {
            const usage = this.processCpuUsage();
            return this.getCpuUsageClass(usage);
        });

        this.totalMemoryTooltip = ko.pureComputed(() => {
            let tooltip = `<div>
                Usable Physical Memory: <strong>${this.sizeFormatter(this.totalMemory())}</strong><br />`;

            if (this.isWindows()) {
                tooltip += ` System Commit Limit: <strong>${this.sizeFormatter(this.systemCommitLimit())}</strong><br />`;
            } else {
                tooltip += ` Swap: <strong>${this.sizeFormatter(this.systemCommitLimit() - this.totalMemory())}</strong><br />`;
            }

            return `${tooltip}</div>`;
        });

        this.machineMemoryUsageTooltip = ko.pureComputed(() => {
            const availableMemory = this.availableMemory();
            const availableMemoryForProcessing = this.availableMemoryForProcessing();

            let tooltip = `<div>
                                Machine Memory Usage: <strong>${this.sizeFormatter(this.usedMemory())}</strong><br />
                                Available Memory: <strong>${this.sizeFormatter(availableMemory)}</strong><br />
                                Available Memory for Processing: <strong>${ this.sizeFormatter(availableMemoryForProcessing) } </strong>`;

            if (this.isWindows()) {
                tooltip += `<br />Commited Memory: <strong>${this.sizeFormatter(this.commitedMemory())}</strong>`;
            }

            return `${tooltip}</div>`;
        });

        this.lowMemoryTooltip = ko.pureComputed(() => {
            let tooltip = `<div><span class="text-warning">Running in ${(this.isExtremelyLowMemory() ? "Extremely " : "")}Low Memory Mode</span>`;

            const availableMemory = this.availableMemory();
            const lowMemoryThreshold = this.lowMemoryThreshold();
            if (availableMemory < lowMemoryThreshold) {
                tooltip += `<br />Available Memory: <strong>${this.sizeFormatter(availableMemory) } </strong>
                            <br />Low Memory Threshold: <strong>${this.sizeFormatter(this.lowMemoryThreshold())}</strong>`;
            }

            if (this.isWindows()) {
                const availableToCommit = this.systemCommitLimit() - this.commitedMemory();
                const commitThreshold = this.commitChargeThreshold();
                if (availableToCommit <= commitThreshold) {
                    tooltip += `<br />Available to Commit: <strong>${this.sizeFormatter(availableToCommit)}</strong>
                                <br /><strong>Commit Threshold: ${this.sizeFormatter(commitThreshold)}</strong><br />`;
                }  
            }

            return `${tooltip}</div>`;
        });
    }

    update(dto: Raven.Server.Dashboard.MachineResources) {
        this.machineCpuUsage(dto.MachineCpuUsage);
        this.processCpuUsage(dto.ProcessCpuUsage);
        this.totalMemory(dto.TotalMemory);
        this.usedMemory(dto.TotalMemory - dto.AvailableMemory);
        this.availableMemory(dto.AvailableMemory);
        this.availableMemoryForProcessing(dto.AvailableMemoryForProcessing);
        this.processMemoryUsage(dto.ProcessMemoryUsage);
        this.systemCommitLimit(dto.SystemCommitLimit);
        this.commitedMemory(dto.CommittedMemory);
        this.isWindows(dto.IsWindows);
        this.isExtremelyLowMemory(dto.LowMemorySeverity === "ExtremelyLow");
        this.isLowMemory(dto.LowMemorySeverity === "Low" || this.isExtremelyLowMemory());
        this.lowMemoryThreshold(dto.LowMemoryThreshold);
        this.commitChargeThreshold(dto.CommitChargeThreshold);
    }

    private getCpuUsageClass(usage: number) {
        if (usage >= 90) {
            return "text-danger";
        } else if (usage >= 80) {
            return "text-warning";
        }

        return "text-success";
    }
}

export = machineResources;
