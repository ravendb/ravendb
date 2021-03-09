import widget = require("viewmodels/resources/widgets/widget");

class cpuUsageWidget extends widget<Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload> {
   
    getType(): Raven.Server.ClusterDashboard.WidgetType {
        return "CpuUsage";
    }
    
    onData(nodeTag: string, data: Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload) {
        console.log("cpu data = ", nodeTag, data);
    }
}

export = cpuUsageWidget;
