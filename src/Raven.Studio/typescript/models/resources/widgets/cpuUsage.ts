import CpuUsagePayload = Raven.Server.Dashboard.Cluster.Notifications.CpuUsagePayload;

import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");

class cpuUsage extends historyAwareNodeStats<CpuUsagePayload> {
    
    ravendbCoresInfo = this.conditionalDataExtractor(x => x.UtilizedCores + "/" + x.NumberOfCores + " Cores", {
        customNoData: "-/- Cores"
    });

    machineCoresInfo = this.conditionalDataExtractor(x => x.NumberOfCores + " Cores", {
        customNoData: "- Cores"
    });
    
    processCpuUsageFormatted = this.conditionalDataExtractor(x => x.ProcessCpuUsage + "%");
    machineCpuUsageFormatted = this.conditionalDataExtractor(x => x.MachineCpuUsage + "%");
}


export = cpuUsage;
