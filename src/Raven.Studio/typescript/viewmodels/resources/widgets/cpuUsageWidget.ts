import widget = require("viewmodels/resources/widgets/widget");

class cpuUsageWidget extends widget<Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload> {
   
    getType(): Raven.Server.ClusterDashboard.WidgetType {
        return "CpuUsage";
    }
    
    compositionComplete() {
        console.log("it works!"); //TODO: please remember you can use such methods!
    }
    
    onData(nodeTag: string, data: Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload) {
        //TODO: console.log("cpu data = ", nodeTag, data);
    }
}

export = cpuUsageWidget;
