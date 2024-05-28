import widget = require("viewmodels/resources/widgets/widget");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import CreateDatabase from "components/pages/resources/databases/partials/create/CreateDatabase";

class welcomeWidget extends widget {

    static clientVersion = viewModelBase.clientVersion;
    
    view = require("views/resources/widgets/welcomeWidget.html");
    
    clusterViewUrl = appUrl.forCluster();
    connectingToDatabaseUrl = welcomeWidget.createLink("GXMEFO");
    indexesUrl = welcomeWidget.createLink("7D62W8");
    queryingUrl = welcomeWidget.createLink("L1QXE3");

    bootstrapped: KnockoutComputed<boolean>;

    isCreateDatabaseViewOpen = ko.observable(false);
    createDatabaseView: ReactInKnockout<typeof CreateDatabase>;

    private static createLink(hash: string) {
        return ko.pureComputed(() => {
            const version = welcomeWidget.clientVersion();
            return `https://ravendb.net/l/${hash}/${version}`;
        });
    }

    constructor(controller: clusterDashboard) {
        super(controller);

        this.bootstrapped = ko.pureComputed(() => this.controller.currentServerNodeTag !== "?");

        this.createDatabaseView = ko.pureComputed(() => ({
            component: CreateDatabase,
            props: {
                closeModal: () => this.isCreateDatabaseViewOpen(false),
            }
        }))
    }
    
    getType(): widgetType {
        return "Welcome";
    }
}

export = welcomeWidget;
