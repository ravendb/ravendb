import viewModelBase = require("viewmodels/viewModelBase");
import removeNodeFromClusterCommand = require("commands/database/cluster/removeNodeFromClusterCommand");
import leaderStepDownCommand = require("commands/database/cluster/leaderStepDownCommand");
import promoteClusterNodeCommand = require("commands/database/cluster/promoteClusterNodeCommand");
import demoteClusterNodeCommand = require("commands/database/cluster/demoteClusterNodeCommand");

import clusterNode = require("models/database/cluster/clusterNode");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import appUrl = require("common/appUrl");

class cluster extends viewModelBase {

    topology = clusterTopologyManager.default.topology;

    canDeleteNodes: KnockoutComputed<boolean>;

    addNodeUrl = appUrl.forAddClusterNode();

    spinners = {
        stepdown: ko.observable<boolean>(false),
        delete: ko.observableArray<string>([]),
        promote: ko.observableArray<string>([]),
        demote: ko.observableArray<string>([])
    }

    constructor() {
        super();
        this.bindToCurrentInstance("deleteNode", "stepDown", "promote", "demote");

        this.initObservables();
    }

    private initObservables() {
        this.canDeleteNodes = ko.pureComputed(() => this.topology().nodes().length > 1);
    }

    activate(args: any) {
        super.activate(args);

        this.updateHelpLink("11HBHO");
    }

    promote(node: clusterNode) {
        this.confirmationMessage("Are you sure?", "Do you want to promote current node to become member/promotable?", ["Cancel", "Yes, promote"])
            .done(result => {
               if (result.can) {
                   this.spinners.promote.push(node.tag());
                   new promoteClusterNodeCommand(node.tag())
                       .execute()
                       .always(() => this.spinners.promote.remove(node.tag()));
               } 
            });
    }

    demote(node: clusterNode) {
         this.confirmationMessage("Are you sure?", "Do you want to demote current node to become watcher?", ["Cancel", "Yes, demote"])
            .done(result => {
               if (result.can) {
                   this.spinners.demote.push(node.tag());
                   new demoteClusterNodeCommand(node.tag())
                       .execute()
                       .always(() => this.spinners.demote.remove(node.tag()));
               } 
            });
    }

    stepDown(node: clusterNode) {
        this.confirmationMessage("Are you sure?", `Do you want current leader to step down?`, ["Cancel", "Step down"])
            .done(result => {
                if (result.can) {
                    this.spinners.stepdown(true);
                    new leaderStepDownCommand()
                        .execute()
                        .always(() => this.spinners.stepdown(false));
                }
            });
    }

    deleteNode(node: clusterNode) {
        this.confirmationMessage("Are you sure?", `Do you want to remove ${node.serverUrl()} from cluster?`, ["Cancel", "Delete"])
            .done(result => {
                if (result.can) {
                    this.spinners.delete.push(node.tag());
                    new removeNodeFromClusterCommand(node.tag())
                        .execute()
                        .always(() => this.spinners.delete.remove(node.tag()));
                }
            });
    }
}

export = cluster;
