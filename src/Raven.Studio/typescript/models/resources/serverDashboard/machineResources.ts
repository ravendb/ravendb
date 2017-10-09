/// <reference path="../../../../typings/tsd.d.ts"/>

class machineResources {
    
    cpuUsage = ko.observable<number>();
    memoryUsage = ko.observable<number>();
    totalMemory = ko.observable<number>();
    
    constructor(dto: Raven.Server.Dashboard.MachineResources) {
        this.update(dto);
    }
    
    update(dto: Raven.Server.Dashboard.MachineResources) {
        this.cpuUsage(dto.CpuUsage);
        this.memoryUsage(dto.MemoryUsage);
        this.totalMemory(dto.TotalMemory);
    }
}

export = machineResources;
