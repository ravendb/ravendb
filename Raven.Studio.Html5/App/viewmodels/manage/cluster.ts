import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import messagePublisher = require("common/messagePublisher");
import topology = require("models/database/replication/topology");
import nodeConnectionInfo = require("models/database/cluster/nodeConnectionInfo");
import editNodeConnectionInfoDialog = require("viewmodels/manage/editNodeConnectionInfoDialog");
import app = require("durandal/app");
import getReducedDatabaseStatsCommand = require("commands/resources/getReducedDatabaseStatsCommand");
import getStatusDebugConfigCommand = require("commands/database/debug/getStatusDebugConfigCommand");
import extendRaftClusterCommand = require("commands/database/cluster/extendRaftClusterCommand");
import initializeNewClusterCommand = require("commands/database/cluster/initializeNewClusterCommand");
import leaveRaftClusterCommand = require("commands/database/cluster/leaveRaftClusterCommand");
import removeClusteringCommand = require("commands/database/cluster/removeClusteringCommand");
import saveClusterConfigurationCommand = require("commands/database/cluster/saveClusterConfigurationCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import updateRaftClusterCommand = require("commands/database/cluster/updateRaftClusterCommand");
import getClusterNodesStatusCommand = require("commands/database/cluster/getClusterNodesStatusCommand");
import shell = require("viewmodels/shell");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");
import license = require("models/auth/license");
import ClusterConfiguration = require("models/database/cluster/clusterConfiguration");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import changeNodeVotingModeCommand = require("commands/database/cluster/changeNodeVotingModeCommand");
import eventsCollector = require("common/eventsCollector");

class cluster extends viewModelBase {

    topology = ko.observable<topology>();
    systemDatabaseId = ko.observable<string>();
    serverUrl = ko.observable<string>(); 
    isLeavingCluster = ko.observable<boolean>(); 
    isReplicationChecksEnabled = ko.observable<boolean>();
    
    canCreateCluster = ko.computed(() => !license.licenseStatus().IsCommercial || license.licenseStatus().Attributes.clustering === "true");
    developerLicense = ko.computed(() => !license.licenseStatus().IsCommercial);
    clusterMode: KnockoutComputed<boolean>;
    settingsAccess = new settingsAccessAuthorizer();
    clusterConfiguration: clusterConfigurationDto;

    constructor() {
        super();
        autoRefreshBindingHandler.install();
        this.clusterMode = ko.computed(() => {
            return this.topology() && this.topology().clusterMode();
        });
        shell.globalChangesApi.watchDocsStartingWith("Raven/Cluster/Configuration",(e)=>{this.updateClusterConfiguration(e)});
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();

        if (this.settingsAccess.isForbidden()) {
            deferred.resolve({ can: true });
        } else {
            var db = appUrl.getSystemDatabase();
            $.when(this.fetchClusterTopology(db), this.fetchDatabaseId(db), this.fetchServerUrl(db), this.fetchClusterConfiguration(db))
                .done(() => {
                    deferred.resolve({ can: true });
                    if (this.clusterMode()) {
                        this.fetchStatus(db);
                    }
                })
                .fail(() => deferred.resolve({ redirect: appUrl.forAdminSettings() }));
        }

        return deferred;
    }

    activate(args) {
        super.activate(args);

        this.updateHelpLink("11HBHO");
    }

    refresh() {
        eventsCollector.default.reportEvent("cluster", "refresh");
        return this.fetchClusterTopology(appUrl.getSystemDatabase())
            .done(() => this.fetchStatus(appUrl.getSystemDatabase()));
    }

    toggleReplicationChecks() {
        var self = this;
        this.clusterConfiguration = {
            EnableReplication: this.clusterConfiguration.EnableReplication,
            DisableReplicationStateChecks: !this.clusterConfiguration.DisableReplicationStateChecks,
            DatabaseSettings: this.clusterConfiguration.DatabaseSettings
        };
        new saveClusterConfigurationCommand(this.clusterConfiguration,
                appUrl.getSystemDatabase())
            .execute()
            .done(() => { self.isReplicationChecksEnabled(!self.isReplicationChecksEnabled()); })
            .fail(() => messagePublisher.reportError("Unable to toggle replication checks."));
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
        return new getReducedDatabaseStatsCommand(db)
            .execute()
            .done((stats: reducedDatabaseStatisticsDto) => {
                this.systemDatabaseId(stats.DatabaseId);
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

    addAnotherServerToCluster() {
        eventsCollector.default.reportEvent("cluster", "add-server");
        var newNode = nodeConnectionInfo.empty();
        var dialog = new editNodeConnectionInfoDialog(newNode, false);
        dialog
            .onExit()
            .done(nci => {
            new extendRaftClusterCommand(appUrl.getSystemDatabase(), nci.toDto(), false)
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
        });
        app.showDialog(dialog);
    }

    removeClustering() {
        eventsCollector.default.reportEvent("cluster", "cleanup");
        this.confirmationMessage("Are you sure?", "You are about to clear cluster information on this server.")
            .done(() => {
                new removeClusteringCommand(appUrl.getSystemDatabase())
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
            new extendRaftClusterCommand(appUrl.getSystemDatabase(), nci.toDto(), true)
                .execute()
                .done(() => {
                    shell.clusterMode(true);
                    setTimeout(() => this.refresh(), 500);
                    new saveClusterConfigurationCommand({ EnableReplication: true,DisableReplicationStateChecks:false }, appUrl.getSystemDatabase())
                        .execute();
                });

        });
        app.showDialog(dialog);
    }

    initializeNewCluster() {
        eventsCollector.default.reportEvent("cluster", "secede");
        this.confirmationMessage("Are you sure?", "You are about to initialize new cluster on this server.")
            .done(() => {
                new initializeNewClusterCommand(appUrl.getSystemDatabase())
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
            });
    }

    editNode(node: nodeConnectionInfo) {
        eventsCollector.default.reportEvent("cluster", "edit");
        var dialog = new editNodeConnectionInfoDialog(node, true);
        dialog.onExit()
            .done((nci: nodeConnectionInfo) => {
                new updateRaftClusterCommand(appUrl.getSystemDatabase(), nci.toDto())
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
            }).always(()=>this.refresh());

        app.showDialog(dialog);
    }

    leaveCluster(node: nodeConnectionInfo) {
        eventsCollector.default.reportEvent("cluster", "leave");
        this.confirmationMessage("Are you sure?", "You are removing node " + node.uri() + " from cluster.")
            .done(() => {
                this.isLeavingCluster(true);
                node.isLeavingCluster(true);
                new leaveRaftClusterCommand(appUrl.getSystemDatabase(), node.toDto())
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500))
                    .always(() => {
                        this.isLeavingCluster(false);
                        node.isLeavingCluster(false);
                    });
            });
    }

    promoteNodeToVoter(node: nodeConnectionInfo) {
        eventsCollector.default.reportEvent("cluster", "promote");
        var nodeAsDto = node.toDto();
        nodeAsDto.IsNoneVoter = false;
        this.confirmationMessage("Are you sure?", "You are promoting node " + node.uri() + " to voter")
            .done(() => {
                new changeNodeVotingModeCommand(appUrl.getSystemDatabase(), nodeAsDto)
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
        });
    }

    private fetchClusterConfiguration(db): JQueryPromise<clusterConfigurationDto> {

        var currentConfiguration: JQueryPromise<clusterConfigurationDto> = new
            getDocumentWithMetadataCommand("Raven/Cluster/Configuration", appUrl.getSystemDatabase(), true)
            .execute()
            .done((result: clusterConfigurationDto) => {
                var notNullResult = result === null ? ClusterConfiguration.empty().toDto() : result;
                this.clusterConfiguration = notNullResult;
                this.isReplicationChecksEnabled(!notNullResult.DisableReplicationStateChecks);
            });

        return currentConfiguration;
    }

    updateClusterConfiguration(documentChangeNotificationDto: documentChangeNotificationDto) {
        this.fetchClusterConfiguration(appUrl.getSystemDatabase());
    }
}

export = cluster;
