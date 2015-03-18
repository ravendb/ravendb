import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import database = require("models/database");
import getClusterTopologyCommand = require("commands/getClusterTopologyCommand");
import messagePublisher = require("common/messagePublisher");
import topology = require("models/topology");
import nodeConnectionInfo = require("models/nodeConnectionInfo");
import editNodeConnectionInfoDialog = require("viewmodels/editNodeConnectionInfoDialog");
import app = require("durandal/app");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getStatusDebugConfigCommand = require("commands/getStatusDebugConfigCommand");
import extendRaftClusterCommand = require("commands/extendRaftClusterCommand");
import leaveRaftClusterCommand = require("commands/leaveRaftClusterCommand");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import clusterConfiguration = require("models/clusterConfiguration");
import saveClusterConfigurationCommand = require("commands/saveClusterConfigurationCommand");
import updateRaftClusterCommand = require("commands/updateRaftClusterCommand");

class cluster extends viewModelBase {

    topology = ko.observable<topology>();
    systemDatabaseId = ko.observable<string>();
	serverUrl = ko.observable<string>();
	isSaveEnabled: KnockoutComputed<boolean>;
	clusterConfiguration = ko.observable<clusterConfiguration>();

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();

        var db = appUrl.getSystemDatabase();
        $.when(this.fetchClusterTopology(db), this.fetchDatabaseId(db), this.fetchServerUrl(db), this.fetchClusterConfiguration(db))
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ redirect: appUrl.forAdminSettings() }));
        return deferred;
	}

	activate(args) {
		super.activate(args);
		this.dirtyFlag = new ko.DirtyFlag([this.clusterConfiguration]);
		this.isSaveEnabled = ko.computed(() => this.dirtyFlag().isDirty());
	}

	fetchClusterConfiguration(db: database): JQueryPromise<any> {
		return new getDocumentWithMetadataCommand("Raven/Cluster/Configuration", db, true)
			.execute()
			.done((result) => this.clusterConfiguration(result ? new clusterConfiguration(result) : clusterConfiguration.empty()))
			.fail(() => messagePublisher.reportError("Unable to fetch cluster configuration"));
	}

    fetchClusterTopology(db: database): JQueryPromise<any> {
        return new getClusterTopologyCommand(db)
            .execute()
            .done(topo => this.topology(topo))
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

    addAnotherServerToCluster() {
        var newNode = nodeConnectionInfo.empty();
        var dialog = new editNodeConnectionInfoDialog(newNode, false);
        dialog
            .onExit()
            .done(nci => {
                new extendRaftClusterCommand(appUrl.getSystemDatabase(), nci.toDto(), false)
                    .execute()
                    .done(() => setTimeout(() => this.fetchClusterTopology(appUrl.getSystemDatabase()), 500));
        });
        app.showDialog(dialog);
    }

    createCluster() {
        var newNode = nodeConnectionInfo.empty();
        newNode.name(this.systemDatabaseId());
        newNode.uri(this.serverUrl());
        var dialog = new editNodeConnectionInfoDialog(newNode, false);
        dialog
            .onExit()
            .done(nci => {
            new extendRaftClusterCommand(appUrl.getSystemDatabase(), nci.toDto(), true)
                .execute()
                .done(() => setTimeout(() => this.fetchClusterTopology(appUrl.getSystemDatabase()), 500));

        });
        app.showDialog(dialog);
    }

	editNode(node: nodeConnectionInfo) {
		var dialog = new editNodeConnectionInfoDialog(node, true);
		dialog.onExit()
			.done(nci => {
				new updateRaftClusterCommand(appUrl.getSystemDatabase(), nci)
					.execute()
					.done(() => setTimeout(() => this.fetchClusterTopology(appUrl.getSystemDatabase()), 500));
			});

		app.showDialog(dialog);
	}

    leaveCluster(node: nodeConnectionInfo) {
        this.confirmationMessage("Are you sure?", "You are removing node " + node.uri() + " from cluster.")
            .done(() => {
                new leaveRaftClusterCommand(appUrl.getSystemDatabase(), node.toDto())
                    .execute()
                    .done(() => setTimeout(() => this.fetchClusterTopology(appUrl.getSystemDatabase()), 500));
        });
	}

	saveClusterConfig() {
		var db = appUrl.getSystemDatabase();
		new saveClusterConfigurationCommand(this.clusterConfiguration().toDto(), db)
			.execute()
			.done(() => this.dirtyFlag().reset());
	}
}

export = cluster;