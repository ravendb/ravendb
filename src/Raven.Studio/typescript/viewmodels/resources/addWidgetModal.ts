import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

interface widgetToAdd {
    name: string;
    icon: "welcome-icon" | "text-icon" | "graph-icon" | "list-icon" | "chart-icon";
    type: widgetType;
}

class addWidgetModal extends dialogViewModelBase {

    private readonly existingTypes: widgetType[];
    private readonly onWidgetSelected: (type: widgetType) => void;
    
    widgets: widgetToAdd[] = [
        {
            name: "Welcome Screen",
            type: "Welcome",
            icon: "welcome-icon"
        },
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
