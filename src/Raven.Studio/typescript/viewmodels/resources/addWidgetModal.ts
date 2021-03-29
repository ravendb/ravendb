import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class addWidgetModal extends dialogViewModelBase {

    private readonly onWidgetSelected: (type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType) => void;
    
    constructor(onWidgetSelected: (type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType) => void) {
        super();
        
        this.onWidgetSelected = onWidgetSelected;
        
        this.bindToCurrentInstance("addWidget");
    }
    
    addWidget(type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType) {
        this.onWidgetSelected(type);
        this.close();
    }
}

export = addWidgetModal;
