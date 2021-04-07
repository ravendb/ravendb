import CpuUsagePayload = Raven.Server.Dashboard.Cluster.Notifications.CpuUsagePayload;

import historyAwareWidget = require("models/resources/widgets/historyAwareWidget");

class cpuUsage extends historyAwareWidget<CpuUsagePayload> {
    
    coresInfo = this.conditionalDataExtractor(x => x.UtilizedCores + "/" + x.NumberOfCores + " Cores", {
        customNoData: "-/- Cores"
    });
    processCpuUsageFormatted = this.conditionalDataExtractor(x => x.ProcessCpuUsage + "%");
    machineCpuUsageFormatted = this.conditionalDataExtractor(x => x.MachineCpuUsage + "%");
    
}


export = cpuUsage;
