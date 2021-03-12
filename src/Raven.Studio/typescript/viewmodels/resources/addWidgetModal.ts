import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class addWidgetModal extends dialogViewModelBase {

    private readonly onWidgetSelected: (type: Raven.Server.ClusterDashboard.WidgetType) => void;
    
    constructor(onWidgetSelected: (type: Raven.Server.ClusterDashboard.WidgetType) => void) {
        super();
        
        this.onWidgetSelected = onWidgetSelected;
        
        this.bindToCurrentInstance("addWidget");
    }
    
    addWidget(type: Raven.Server.ClusterDashboard.WidgetType) {
        this.onWidgetSelected(type);
        this.close();
    }
}

export = addWidgetModal;
