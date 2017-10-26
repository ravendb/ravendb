/// <reference path="../../../../typings/tsd.d.ts"/>

class machineResources {
    cpuUsage = ko.observable<number>();
    ravenCpuUsage = ko.observable<number>();
    memoryUsage = ko.observable<number>();
    ravenMemoryUsage = ko.observable<number>();
    totalMemory = ko.observable<number>();
    
    cpuUsageClass: KnockoutComputed<string>;
    ravenCpuUsageClass: KnockoutComputed<string>;
    memoryUsageClass: KnockoutComputed<string>;
    ravenMemoryUsageClass: KnockoutComputed<string>;
    
    constructor(dto: Raven.Server.Dashboard.MachineResources) {
        this.update(dto);
        
        this.cpuUsageClass = ko.pureComputed(() => {
            const usage = this.cpuUsage();
            return this.getCpuUsageClass(usage);
        });

        this.ravenCpuUsageClass = ko.pureComputed(() => {
            const usage = this.ravenCpuUsage();
            return this.getCpuUsageClass(usage);
        });
        
        this.memoryUsageClass = ko.pureComputed(() => {
            const used = this.memoryUsage();
            return this.getMemoryUsageClass(used);
        });

        this.ravenMemoryUsageClass = ko.pureComputed(() => {
            const used = this.ravenMemoryUsage();
            return this.getMemoryUsageClass(used);
        });
    }
    
    update(dto: Raven.Server.Dashboard.MachineResources) {
        this.cpuUsage(dto.CpuUsage);
        this.ravenCpuUsage(dto.RavenCpuUsage);
        this.memoryUsage(dto.MemoryUsage);
        this.ravenMemoryUsage(dto.RavenMemoryUsage);
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
