import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import addNodeToClusterCommand = require("commands/database/cluster/addNodeToClusterCommand");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import addClusterNodeModel = require("models/database/cluster/addClusterNodeModel");
import testClusterNodeConnectionCommand = require("commands/database/cluster/testClusterNodeConnectionCommand");

class addClusterNode extends viewModelBase {

    model = new addClusterNodeModel();

    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();

    spinners = {
        save: ko.observable<boolean>(false),
        test: ko.observable<boolean>(false)
    }

    constructor() {
        super();

        this.bindToCurrentInstance("testConnection", "save");

        // discard test connection result when url has changed
        this.model.serverUrl.subscribe(() => this.testConnectionResult(null));
    }

    save() {
        if (this.isValid(this.model.validationGroup)) {
            eventsCollector.default.reportEvent("cluster", "add-server");

            this.spinners.save(true);

            new addNodeToClusterCommand(this.model.serverUrl(), this.model.addAsWatcher())
                .execute()
                .done(() => this.goToClusterView())
                .always(() => this.spinners.save(false));
        }
    }

    testConnection() {
        if (this.isValid(this.model.validationGroup)) { 
            eventsCollector.default.reportEvent("cluster", "test-connection");

            this.spinners.test(true);

            new testClusterNodeConnectionCommand(this.model.serverUrl())
                .execute()
                .done(result => this.testConnectionResult(result))
                .always(() => this.spinners.test(false));
        }
    }

    cancelOperation() {
        this.goToClusterView();
    }

    private goToClusterView() {
        router.navigate(appUrl.forCluster());
    }
}

export = addClusterNode;
