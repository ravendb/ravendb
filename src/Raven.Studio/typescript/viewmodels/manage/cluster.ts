import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import messagePublisher = require("common/messagePublisher");
import nodeConnectionInfo = require("models/database/cluster/nodeConnectionInfo");
import editNodeConnectionInfoDialog = require("viewmodels/manage/editNodeConnectionInfoDialog");
import app = require("durandal/app");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getStatusDebugConfigCommand = require("commands/database/debug/getStatusDebugConfigCommand");
import extendRaftClusterCommand = require("commands/database/cluster/extendRaftClusterCommand");
import initializeNewClusterCommand = require("commands/database/cluster/initializeNewClusterCommand");
import leaveRaftClusterCommand = require("commands/database/cluster/leaveRaftClusterCommand");
import removeClusteringCommand = require("commands/database/cluster/removeClusteringCommand");
import saveClusterConfigurationCommand = require("commands/database/cluster/saveClusterConfigurationCommand");
import updateRaftClusterCommand = require("commands/database/cluster/updateRaftClusterCommand");
import getClusterNodesStatusCommand = require("commands/database/cluster/getClusterNodesStatusCommand");
import shell = require("viewmodels/shell");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import changeNodeVotingModeCommand = require("commands/database/cluster/changeNodeVotingModeCommand");
import eventsCollector = require("common/eventsCollector");
import addNodeToClusterCommand = require("commands/database/cluster/addNodeToClusterCommand");
import removeNodeFromClusterCommand = require("commands/database/cluster/removeNodeFromClusterCommand");

import clusterTopology = require("models/database/cluster/clusterTopology");
import clusterNode = require("models/database/cluster/clusterNode");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class cluster extends viewModelBase {

    topology = clusterTopologyManager.default.topology;

    canDeleteNodes: KnockoutComputed<boolean>;

    constructor() {
        super();
        this.bindToCurrentInstance("deleteNode");

        this.initObservables();
    }

    private initObservables() {
        this.canDeleteNodes = ko.pureComputed(() => this.topology().nodes().length > 1);
    }

    addAnotherServerToCluster() {
        eventsCollector.default.reportEvent("cluster", "add-server");

        const serverUrl = prompt("Enter server URL:");
        if (serverUrl) {
            new addNodeToClusterCommand(serverUrl)
                .execute();
        }
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

    /* TODO
    topology = ko.observable<topology>();
    systemDatabaseId = ko.observable<string>();
    serverUrl = ko.observable<string>(); 
    
    canCreateCluster = ko.computed(() => !license.licenseStatus().IsCommercial || license.licenseStatus().Attributes.clustering === "true");
    developerLicense = ko.computed(() => !license.licenseStatus().IsCommercial);
    clusterMode: KnockoutComputed<boolean>;

    constructor() {
        super();
        autoRefreshBindingHandler.install();
        this.clusterMode = ko.computed(() => {
            return this.topology() && this.topology().clusterMode();
        });
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();

        if (settingsAccessAuthorizer.isForbidden()) {
            deferred.resolve({ can: true });
        } else {
            $.when(this.fetchClusterTopology(null), this.fetchDatabaseId(null), this.fetchServerUrl(null))
                .done(() => {
                    deferred.resolve({ can: true });
                    if (this.clusterMode()) {
                        this.fetchStatus(null);
                    }
                })
                .fail(() => deferred.resolve({ redirect: appUrl.forAdminSettings() }));
        }

        return deferred;
    }

    activate(args: any) {
        super.activate(args);

        this.updateHelpLink("11HBHO");
    }

    refresh() {
        eventsCollector.default.reportEvent("cluster", "refresh");

        return this.fetchClusterTopology(null)
            .done(() => this.fetchStatus(null));
    }

    fetchClusterTopology(db: database): JQueryPromise<any> {
        return new getClusterTopologyCommand(db)
            .execute()
            .done(topo => {
                this.topology(topo);
            })
            .fail(() => messagePublisher.reportError("Unable to fetch cluster topology"));
    }

    fetchDatabaseId(db: database): JQueryPromise<any> {
        return new getDatabaseStatsCommand(db)
            .execute()
            .done((stats) => {
                this.systemDatabaseId(stats.DatabaseId); //TODO: make sure it works!
            });
    }

    fetchServerUrl(db: database): JQueryPromise<any> {
        return new getStatusDebugConfigCommand(db)
            .execute()
            .done(config => this.serverUrl(config.ServerUrl));
    }

    fetchStatus(db: database): JQueryPromise<any> {
        return new getClusterNodesStatusCommand(db)
            .execute()
            .done((status) => this.updateNodesStatus(status));
    }

    updateNodesStatus(status: Array<clusterNodeStatusDto>) {
        status.forEach(nodeStatus => {
            var nci = this.topology().allNodes().find(n => n.uri() === nodeStatus.Uri);
            if (nci) {
                nci.status(nodeStatus.Status);
            }
        });
    }

    addAnotherServerToCluster(forcedAdd: boolean) {
        

        var newNode = nodeConnectionInfo.empty();
        var dialog = new editNodeConnectionInfoDialog(newNode, false);
        dialog
            .onExit()
            .done(nci => {
            new extendRaftClusterCommand(null, nci.toDto(), false, forcedAdd)
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
        });
        app.showBootstrapDialog(dialog);
    }

    removeClustering() {
        eventsCollector.default.reportEvent("cluster", "cleanup");

        this.confirmationMessage("Are you sure?", "You are about to clear cluster information on this server.")
            .done(() => {
                new removeClusteringCommand(null)
                    .execute()
                    .done(() => setTimeout(() => {
                        this.refresh();
                        shell.clusterMode(false);
                    }, 500));
            });
    }

    createCluster() {
        eventsCollector.default.reportEvent("cluster", "create");

        var newNode = nodeConnectionInfo.empty();
        newNode.name(this.systemDatabaseId());
        newNode.uri(this.serverUrl());
        var dialog = new editNodeConnectionInfoDialog(newNode, false);
        dialog
            .onExit()
            .done(nci => {
            new extendRaftClusterCommand(null, nci.toDto(), true, false)
                .execute()
                .done(() => {
                    shell.clusterMode(true);
                    setTimeout(() => this.refresh(), 500);
                    new saveClusterConfigurationCommand({ EnableReplication: true }, null)
                        .execute();
                });

        });
        app.showBootstrapDialog(dialog);
    }

    initializeNewCluster() {
        eventsCollector.default.reportEvent("cluster", "secede");

        this.confirmationMessage("Are you sure?", "You are about to initialize new cluster on this server.")
            .done(() => {
                new initializeNewClusterCommand(null)
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
            });
    }

    editNode(node: nodeConnectionInfo) {
        eventsCollector.default.reportEvent("cluster", "edit");

        var dialog = new editNodeConnectionInfoDialog(node, true);
        dialog.onExit()
            .done((nci: nodeConnectionInfo) => {
                new updateRaftClusterCommand(null, nci.toDto())
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
            });

        app.showBootstrapDialog(dialog);
    }

    leaveCluster(node: nodeConnectionInfo) {
        eventsCollector.default.reportEvent("cluster", "leave");

        this.confirmationMessage("Are you sure?", "You are removing node " + node.uri() + " from cluster.")
            .done(() => {
                new leaveRaftClusterCommand(null, node.toDto())
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
        });
    }

    promoteNodeToVoter(node: nodeConnectionInfo) {
        eventsCollector.default.reportEvent("cluster", "promote");

        var nodeAsDto = node.toDto();
        nodeAsDto.IsNoneVoter = false;
        this.confirmationMessage("Are you sure?", "You are promoting node " + node.uri() + " to voter.")
            .done(() => {
                new changeNodeVotingModeCommand(null, nodeAsDto)
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
        });
    }*/
}

export = cluster;
