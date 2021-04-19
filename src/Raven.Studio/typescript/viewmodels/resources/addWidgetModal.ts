import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

interface widgetToAdd {
    name: string;
    icon: "text-icon" | "graph-icon" | "list-icon" | "chart-icon";
    type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType;
}

class addWidgetModal extends dialogViewModelBase {

    private readonly existingTypes: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType[];
    private readonly onWidgetSelected: (type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType) => void;
    
    widgets: widgetToAdd[] = [
        {
            name: "License info",
            type: "License",
            icon: "text-icon"
        }, {
            name: "CPU",
            icon: "graph-icon",
            type: "CpuUsage"
        }, {
            name: "Memory",
            icon: "graph-icon",
            type: "MemoryUsage"
        }, {
            name: "Indexing",
            icon: "graph-icon",
            type: "Indexing"
        }, {
            name: "Traffic",
            icon: "list-icon",
            type: "Traffic"
        }, {
            name: "Indexing per database", 
            icon: "list-icon",
            type: "DatabaseIndexing"
        }, {
            name: "Storage per database",
            icon: "list-icon",
            type: "DatabaseStorageUsage"
        }, {
            name: "Storage",
            icon: "chart-icon",
            type: "StorageUsage"
        }, {
            name: "Traffic per database",
            icon: "list-icon",
            type: "DatabaseTraffic"
        }
    ]
    
    constructor(
        existingTypes: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType[], 
        onWidgetSelected: (type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType) => void
    ) {
        super();
        
        this.existingTypes = existingTypes;
        this.onWidgetSelected = onWidgetSelected;
        
        this.bindToCurrentInstance("addWidget");
    }
    
    addWidget(type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType) {
        if (!this.canAddWidget(type)) {
            return;
        }
        this.onWidgetSelected(type);
        this.close();
    }
    
    compositionComplete(view?: any, parent?: any) {
        super.compositionComplete(view, parent);
        
        $(".widget-item.disabled").tooltip({
            title: "Already added"
        });
    }

    canAddWidget(type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType) {
        return !_.includes(this.existingTypes, type);
    }
}

export = addWidgetModal;
