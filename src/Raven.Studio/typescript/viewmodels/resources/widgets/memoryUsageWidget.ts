import widget = require("viewmodels/resources/widgets/widget");

type MemoryWidgetPayload = Raven.Server.ClusterDashboard.Widgets.MemoryBasicUsagePayload | Raven.Server.ClusterDashboard.Widgets.MemoryExtendedUsagePayload;

class memoryUsageWidget extends widget<MemoryWidgetPayload> {

    getType(): Raven.Server.ClusterDashboard.WidgetType {
        return "MemoryUsage";
    }

    onData(nodeTag: string, data: MemoryWidgetPayload) {
        switch (data.Type) {
            case "Basic":
                this.onBasicData(nodeTag, data as Raven.Server.ClusterDashboard.Widgets.MemoryBasicUsagePayload);
                break;
            case "Extended":
                this.onExtendedData(nodeTag, data as Raven.Server.ClusterDashboard.Widgets.MemoryExtendedUsagePayload);
                break;
        }
    }
    
    private onBasicData(nodeTag: string, data: Raven.Server.ClusterDashboard.Widgets.MemoryBasicUsagePayload) {
        //TODO: console.log("basic memory data = ", nodeTag, data);
    }
    
    private onExtendedData(nodeTag: string, data: Raven.Server.ClusterDashboard.Widgets.MemoryExtendedUsagePayload) {
        //TODO: console.log("extended memory data = ", nodeTag, data);
    }
}

export = memoryUsageWidget;
