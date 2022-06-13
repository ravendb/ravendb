import widget = require("viewmodels/resources/widgets/widget");
import app = require("durandal/app");
import appUrl = require("common/appUrl");
import createDatabase = require("viewmodels/resources/createDatabase");
import viewModelBase = require("viewmodels/viewModelBase");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");

class welcomeWidget extends widget {

    static clientVersion = viewModelBase.clientVersion;
    
    view = require("views/resources/widgets/welcomeWidget.html");
    
    clusterViewUrl = appUrl.forCluster();
    connectingToDatabaseUrl = welcomeWidget.createLink("GXMEFO");
    indexesUrl = welcomeWidget.createLink("7D62W8");
    queryingUrl = welcomeWidget.createLink("L1QXE3");

    bootstrapped: KnockoutComputed<boolean>;
    
    private static createLink(hash: string) {
        return ko.pureComputed(() => {
            const version = welcomeWidget.clientVersion();
            return `https://ravendb.net/l/${hash}/${version}`;
        });
    }

    constructor(controller: clusterDashboard) {
        super(controller);

        this.bootstrapped = ko.pureComputed(() => this.controller.currentServerNodeTag !== "?");
    }
    
    getType(): widgetType {
        return "Welcome";
    }

    newDatabase() {
        const createDbView = new createDatabase("newDatabase");
        app.showBootstrapDialog(createDbView);
    }
}

export = welcomeWidget;
