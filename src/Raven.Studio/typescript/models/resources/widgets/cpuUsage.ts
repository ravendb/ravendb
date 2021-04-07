/// <reference path="../../../../typings/tsd.d.ts"/>

import CpuUsagePayload = Raven.Server.Dashboard.Cluster.Notifications.CpuUsagePayload;

import historyAwareWidget = require("models/resources/widgets/historyAwareWidget");

class cpuUsage extends historyAwareWidget<CpuUsagePayload> {
    
    coresInfo: KnockoutComputed<string>;
    processCpuUsageFormatted: KnockoutComputed<string>;
    machineCpuUsageFormatted: KnockoutComputed<string>;

    constructor(tag: string) {
        super(tag);

        this.coresInfo = ko.pureComputed(() => {
            const noData = this.noDataText();
            if (noData) {
                return "-/- Cores";
            }
            const dto = this.currentItem().value;
            return dto.UtilizedCores + "/" + dto.NumberOfCores + " Cores";
        });

        this.processCpuUsageFormatted = ko.pureComputed(() => {
            const noData = this.noDataText();
            if (noData) {
                return noData;
            }
            const dto = this.currentItem().value;
            return dto.ProcessCpuUsage + "%";
        });

        this.machineCpuUsageFormatted = ko.pureComputed(() => {
            const noData = this.noDataText();
            if (noData) {
                return noData;
            }
            const dto = this.currentItem().value;
            return dto.MachineCpuUsage + "%";
        });
    }
}


export = cpuUsage;
