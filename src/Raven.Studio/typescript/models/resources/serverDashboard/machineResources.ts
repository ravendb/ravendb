/// <reference path="../../../../typings/tsd.d.ts"/>

class machineResources {
    machineCpuUsage = ko.observable<number>(0);
    processCpuUsage = ko.observable<number>(0);
    machineMemoryUsage = ko.observable<number>(0);
    processMemoryUsage = ko.observable<number>(0);
    totalMemory = ko.observable<number>(0);
    
    machineCpuUsageClass: KnockoutComputed<string>;
    processCpuUsageClass: KnockoutComputed<string>;
    machineMemoryUsageClass: KnockoutComputed<string>;
    processMemoryUsageClass: KnockoutComputed<string>;
    
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
        
        this.machineMemoryUsageClass = ko.pureComputed(() => {
            const used = this.machineMemoryUsage();
            return this.getMemoryUsageClass(used);
        });

        this.processMemoryUsageClass = ko.pureComputed(() => {
            const used = this.processMemoryUsage();
            return this.getMemoryUsageClass(used);
        });
    }
    
    update(dto: Raven.Server.Dashboard.MachineResources) {
        this.machineCpuUsage(dto.MachineCpuUsage);
        this.processCpuUsage(dto.ProcessCpuUsage);
        this.machineMemoryUsage(dto.MachineMemoryUsage);
        this.processMemoryUsage(dto.ProcessMemoryUsage);
        this.totalMemory(dto.TotalMemory);
    }

    private getCpuUsageClass(usage: number) {
        if (usage >= 90) {
            return "text-danger";
        } else if (usage >= 80) {
            return "text-warning";
        }

        return "text-success";
    }

    private getMemoryUsageClass(used: number): string {
        const total = this.totalMemory();
        if (!total) {
            return "";
        }

        const percentageUsage = used * 100.0 / total;
        if (percentageUsage >= 90) {
            return "text-danger";
        } else if (percentageUsage >= 80) {
            return "text-warning";
        }

        return "text-success";
    }
}

export = machineResources;
