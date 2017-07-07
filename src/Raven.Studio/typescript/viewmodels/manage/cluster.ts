import viewModelBase = require("viewmodels/viewModelBase");
import eventsCollector = require("common/eventsCollector");
import addNodeToClusterCommand = require("commands/database/cluster/addNodeToClusterCommand");
import removeNodeFromClusterCommand = require("commands/database/cluster/removeNodeFromClusterCommand");
import leaderStepDownCommand = require("commands/database/cluster/leaderStepDownCommand");

import clusterNode = require("models/database/cluster/clusterNode");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class cluster extends viewModelBase {

    topology = clusterTopologyManager.default.topology;

    canDeleteNodes: KnockoutComputed<boolean>;

    constructor() {
        super();
        this.bindToCurrentInstance("deleteNode", "stepDown");

        this.initObservables();
    }

    private initObservables() {
        this.canDeleteNodes = ko.pureComputed(() => this.topology().nodes().length > 1);
    }

    activate(args: any) {
        super.activate(args);

        this.updateHelpLink("11HBHO");
    }

    addAnotherServerToCluster() {
        eventsCollector.default.reportEvent("cluster", "add-server");

        const serverUrl = prompt("Enter server URL:");
        if (serverUrl) {
            // TODO: use url validation from extensions.ts when implementing the dialog instead of the prompt
            new addNodeToClusterCommand(serverUrl)
                .execute();
        }
    }

    stepDown(node: clusterNode) {
        this.confirmationMessage("Are you sure?", `Do you want current leader to step down?`, ["Cancel", "Step down"])
            .done(result => {
                if (result.can) {
                    new leaderStepDownCommand()
                        .execute();
                }
            });
    }

    deleteNode(node: clusterNode) {
        this.confirmationMessage("Are you sure?", `Do you want to remove ${node.serverUrl()} from cluster?`, ["Cancel", "Delete"])
            .done(result => {
                if (result.can) {
                    new removeNodeFromClusterCommand(node.tag())
                        .execute();
                }
            });
    }
}

export = cluster;
