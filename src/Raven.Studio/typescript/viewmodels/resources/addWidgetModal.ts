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
            type: "Welcome",
            icon: "welcome-icon"
        }, {
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
            name: "Indexing per Database",
            icon: "list-icon",
            type: "DatabaseIndexing"
        }, {
            name: "Traffic",
            icon: "list-icon",
            type: "Traffic"
        }, {
            name: "Traffic per Database",
            icon: "list-icon",
            type: "DatabaseTraffic"
        }, {
            name: "Storage",
            icon: "chart-icon",
            type: "StorageUsage"
        }, {
            name: "Storage per Database",
            icon: "list-icon",
            type: "DatabaseStorageUsage"
        }, {
            name: "Database Overview",
            icon: "list-icon",
            type: "DatabaseOverview"
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
