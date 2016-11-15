import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import messagePublisher = require("common/messagePublisher");
import topology = require("models/database/replication/topology");
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
import license = require("models/auth/license");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import changeNodeVotingModeCommand = require("commands/database/cluster/changeNodeVotingModeCommand");

class cluster extends viewModelBase {

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
            var nci = this.topology().allNodes().first(n => n.uri() === nodeStatus.Uri);
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
        this.confirmationMessage("Are you sure?", "You are about to initialize new cluster on this server.")
            .done(() => {
                new initializeNewClusterCommand(null)
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
            });
    }

    editNode(node: nodeConnectionInfo) {
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
        this.confirmationMessage("Are you sure?", "You are removing node " + node.uri() + " from cluster.")
            .done(() => {
                new leaveRaftClusterCommand(null, node.toDto())
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
        });
    }

    promoteNodeToVoter(node: nodeConnectionInfo) {
        var nodeAsDto = node.toDto();
        nodeAsDto.IsNoneVoter = false;
        this.confirmationMessage("Are you sure?", "You are promoting node " + node.uri() + " to voter.")
            .done(() => {
                new changeNodeVotingModeCommand(null, nodeAsDto)
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
        });
    }
}

export = cluster;
