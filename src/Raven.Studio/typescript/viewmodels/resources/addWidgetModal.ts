import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

interface widgetToAdd {
    name: string;
    icon: "welcome-icon" | "text-icon" | "graph-icon" | "list-icon" | "chart-icon";
    type: widgetType;
}

class addWidgetModal extends dialogViewModelBase {

    view = require("views/resources/addWidgetModal.html");
    
    private readonly existingTypes: widgetType[];
    private readonly onWidgetSelected: (type: widgetType) => void;
    
    widgets: widgetToAdd[] = [
        {
            name: "Welcome Screen",
            icon: require("../../../wwwroot/Content/img/widgets/welcome-icon.svg"),
            type: "Welcome"
        }, {
            name: "License info",
            icon: require("../../../wwwroot/Content/img/widgets/text-icon.svg"),
            type: "License"
        }, {
            name: "Cluster Overview",
            icon: require("../../../wwwroot/Content/img/widgets/chart-icon.svg"),
            type: "ClusterOverview"            
        }, {
            name: "Storage",
            icon: require("../../../wwwroot/Content/img/widgets/chart-icon.svg"),
            type: "StorageUsage"
        }, {
            name: "CPU",
            icon: require("../../../wwwroot/Content/img/widgets/graph-icon.svg"),
            type: "CpuUsage"
        }, {
            name: "Memory",
            icon: require("../../../wwwroot/Content/img/widgets/graph-icon.svg"),
            type: "MemoryUsage"
        }, {
            name: "Indexing",
            icon: require("../../../wwwroot/Content/img/widgets/graph-icon.svg"),
            type: "Indexing"
        }, {
            name: "Traffic",
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "Traffic"
        }, {
            name: "Indexing per Database",
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "DatabaseIndexing"
        }, {
            name: "Traffic per Database",
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "DatabaseTraffic"
        }, {
            name: "Storage per Database",
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "DatabaseStorageUsage"
        }, {
            name: "Database Overview",
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "DatabaseOverview"
        }, {
            name: "Ongoing Tasks",
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "OngoingTasks"
        }
    ]
    
    constructor(
        existingTypes: widgetType[], 
        onWidgetSelected: (type: widgetType) => void
    ) {
        super();
        
        this.existingTypes = existingTypes;
        this.onWidgetSelected = onWidgetSelected;
        
        this.bindToCurrentInstance("addWidget");
    }
    
    addWidget(type: widgetType) {
        if (!this.canAddWidget(type)) {
            return;
        }
        this.onWidgetSelected(type);
        this.close();
    }

    addAllWidgets() {
        this.widgets.forEach(x => {
           if (this.canAddWidget(x.type)) {
               this.onWidgetSelected(x.type);
           } 
        });
        this.close();
    }
    
    compositionComplete(view?: any, parent?: any) {
        super.compositionComplete(view, parent);
        
        $(".widget-item.disabled").tooltip({
            title: "Already added"
        });
    }

    canAddWidget(type: widgetType) {
        return !_.includes(this.existingTypes, type);
    }
}

export = addWidgetModal;
