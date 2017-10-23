/// <reference path="../../../../typings/tsd.d.ts"/>

class machineResources {
    
    cpuUsage = ko.observable<number>();
    memoryUsage = ko.observable<number>();
    totalMemory = ko.observable<number>();
    
    cpuUsageClass: KnockoutComputed<string>;
    memoryUsageClass: KnockoutComputed<string>;
    
    constructor(dto: Raven.Server.Dashboard.MachineResources) {
        this.update(dto);
        
        this.cpuUsageClass = ko.pureComputed(() => {
           const usage = this.cpuUsage();
           
           if (usage >= 90) {
               return "text-danger";
           } else if (usage >= 80) {
               return "text-warning";
           } else {
               return "text-success";
           }
        });
        
        this.memoryUsageClass = ko.pureComputed(() => {
           const used = this.memoryUsage();
           const total = this.totalMemory();
           
           if (!total) {
               return "";
           }
           
           const percentageUsage = used * 100.0 / total;
           if (percentageUsage >= 90) {
               return "text-danger";
           } else if (percentageUsage >= 80) {
               return "text-warning";
           } else {
               return "text-success";
           }
        });
    }
    
    update(dto: Raven.Server.Dashboard.MachineResources) {
        this.cpuUsage(dto.CpuUsage);
        this.memoryUsage(dto.MemoryUsage);
        this.totalMemory(dto.TotalMemory);
    }
}

export = machineResources;
