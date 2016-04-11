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
import saveClusterConfigurationCommand = require("commands/database/cluster/saveClusterConfigurationCommand");
import updateRaftClusterCommand = require("commands/database/cluster/updateRaftClusterCommand");
import getClusterNodesStatusCommand = require("commands/database/cluster/getClusterNodesStatusCommand");
import shell = require("viewmodels/shell");

class cluster extends viewModelBase {

    topology = ko.observable<topology>();
    systemDatabaseId = ko.observable<string>();
    serverUrl = ko.observable<string>();

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();

        var db = null;
        $.when(this.fetchClusterTopology(db), this.fetchDatabaseId(db), this.fetchServerUrl(db))
            .done(() => {
                deferred.resolve({ can: true });
                this.fetchStatus(db);
            })
            .fail(() => deferred.resolve({ redirect: appUrl.forAdminSettings() }));
        return deferred;
    }

    refresh() {
       /* this.fetchClusterTopology(appUrl.getSystemDatabase())
            .done(() => this.fetchStatus(appUrl.getSystemDatabase()));*/
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
            .done((stats: databaseStatisticsDto) => {
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

    addAnotherServerToCluster(forcedAdd: boolean) {
       /* var newNode = nodeConnectionInfo.empty();
        var dialog = new editNodeConnectionInfoDialog(newNode, false);
        dialog
            .onExit()
            .done(nci => {
            new extendRaftClusterCommand(appUrl.getSystemDatabase(), nci.toDto(), false, forcedAdd)
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
        });
        app.showDialog(dialog);*/
    }

    createCluster() {
        /*var newNode = nodeConnectionInfo.empty();
        newNode.name(this.systemDatabaseId());
        newNode.uri(this.serverUrl());
        var dialog = new editNodeConnectionInfoDialog(newNode, false);
        dialog
            .onExit()
            .done(nci => {
            new extendRaftClusterCommand(appUrl.getSystemDatabase(), nci.toDto(), true, false)
                .execute()
                .done(() => {
                    shell.clusterMode(true);
                    setTimeout(() => this.refresh(), 500);
                    new saveClusterConfigurationCommand({ EnableReplication: true }, appUrl.getSystemDatabase())
                        .execute();
                });

        });
        app.showDialog(dialog);*/
    }

    initializeNewCluster() {
       /* this.confirmationMessage("Are you sure?", "You are about to initialize new cluster on this server.")
            .done(() => {
            new initializeNewClusterCommand(appUrl.getSystemDatabase())
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
            });*/
    }

    editNode(node: nodeConnectionInfo) {
        /*var dialog = new editNodeConnectionInfoDialog(node, true);
        dialog.onExit()
            .done(nci => {
                new updateRaftClusterCommand(appUrl.getSystemDatabase(), nci.toDto())
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
            });

        app.showDialog(dialog);*/
    }

    leaveCluster(node: nodeConnectionInfo) {
       /* this.confirmationMessage("Are you sure?", "You are removing node " + node.uri() + " from cluster.")
            .done(() => {
                new leaveRaftClusterCommand(appUrl.getSystemDatabase(), node.toDto())
                    .execute()
                    .done(() => setTimeout(() => this.refresh(), 500));
        });*/
    }
}

export = cluster;
