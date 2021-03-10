import widget = require("viewmodels/resources/widgets/widget");

//TODO: avoid any
class licenseWidget extends widget<any> {

    getType(): Raven.Server.ClusterDashboard.WidgetType {
        return "License";
    }
    
    onData(nodeTag: string, data: any) {
        //TODO: types!
    }
    
    supportedOnNode(targetNodeTag: string, currentServerNodeTag: string): boolean {
        return targetNodeTag === currentServerNodeTag;
    }

}

export = licenseWidget;
