import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import addNodeToClusterCommand = require("commands/database/cluster/addNodeToClusterCommand");
import removeNodeFromClusterCommand = require("commands/database/cluster/removeNodeFromClusterCommand");
import leaderStepDownCommand = require("commands/database/cluster/leaderStepDownCommand");

import clusterNode = require("models/database/cluster/clusterNode");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import addClusterNodeModel = require("models/database/cluster/addClusterNodeModel");

class addClusterNode extends viewModelBase {

    model = new addClusterNodeModel();

    spinners = {
        save: ko.observable<boolean>(false)
    }

    save() {
        if (this.isValid(this.model.validationGroup)) {
            eventsCollector.default.reportEvent("cluster", "add-server");

            this.spinners.save(true);

            new addNodeToClusterCommand(this.model.serverUrl())
                .execute()
                .done(() => this.goToClusterView())
                .always(() => this.spinners.save(false));
        }
    }

    testConnection() {
        //TODO:
    }

    cancelOperation() {
        this.goToClusterView();
    }

    private goToClusterView() {
        router.navigate(appUrl.forCluster());
    }
}

export = addClusterNode;
