import viewModelBase = require("viewmodels/viewModelBase");

import app = require("durandal/app");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import databaseInfo = require("models/resources/info/databaseInfo");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import addNodeToDatabaseGroupCommand = require("commands/database/dbGroup/addNodeToDatabaseGroupCommand");
import databaseGroupNode = require("models/resources/info/databaseGroupNode");
import deleteDatabaseFromNodeCommand = require("commands/resources/deleteDatabaseFromNodeCommand");
import databaseGroupGraph = require("models/database/dbGroup/databaseGroupGraph");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import toggleDynamicNodeAssignmentCommand = require("commands/database/dbGroup/toggleDynamicNodeAssignmentCommand");
import showDataDialog = require("viewmodels/common/showDataDialog");
import defineNodeEncryptionKey = require("viewmodels/resources/defineNodeEncryptionKey");

class manageDatabaseGroup extends viewModelBase {

    currentDatabaseInfo = ko.observable<databaseInfo>();
    selectedClusterNode = ko.observable<string>();

    private graph = new databaseGroupGraph();

    nodeTag = clusterTopologyManager.default.localNodeTag;
    nodes: KnockoutComputed<databaseGroupNode[]>;
    additionalNodes: KnockoutComputed<string[]>;
    addNodeEnabled: KnockoutComputed<boolean>;

    spinners = {
        addNode: ko.observable<boolean>(false)
    };

    constructor() {
        super();

        this.bindToCurrentInstance("addNode", "deleteNodeFromGroup", "showErrorDetails");

        this.initObservables();
    }

    private initObservables() {
        this.nodes = ko.pureComputed(() => {
            const dbInfo = this.currentDatabaseInfo();
            return dbInfo.nodes();
        });

        this.additionalNodes = ko.pureComputed<string[]>(() => {
            const tags = clusterTopologyManager.default.topology().nodes().map(x => x.tag());
            const existingTags = this.nodes().map(x => x.tag());
            return _.without(tags, ...existingTags);
        });

        this.addNodeEnabled = ko.pureComputed(() => {
            const nodeSelected = !!this.selectedClusterNode();
            const inProgress = this.spinners.addNode();
            return nodeSelected && !inProgress;
        });
    }

    activate(args: any) {
        super.activate(args);

        this.addNotification(this.changesContext.serverNotifications()
            .watchAllDatabaseChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.refresh()));

        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
    }

    compositionComplete(): void {
        super.compositionComplete();

        this.registerDisposableHandler($(document), "fullscreenchange", () => {
            $("body").toggleClass("fullscreen", $(document).fullScreen());
            this.graph.onResize();
        });

        this.graph.init($("#databaseGroupGraphContainer"));
    }

    private refresh() {
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
    }
    
    private fetchDatabaseInfo() {
        return new getDatabaseCommand(this.activeDatabase().name)
            .execute()
            .done(dbInfo => {
                this.graph.onDatabaseInfoChanged(dbInfo);
                this.onDatabaseInfoFetched(dbInfo);
            });
    }

    private fetchOngoingTasks(): JQueryPromise<Raven.Server.Web.System.OngoingTasksResult> {
        const db = this.activeDatabase();
        return new ongoingTasksCommand(db)
            .execute()
            .done((info) => {
                this.graph.onTasksChanged(info);
            });
    }

    private onDatabaseInfoFetched(dbInfoDto: Raven.Client.ServerWide.Operations.DatabaseInfo) {
        const dbInfo = new databaseInfo(dbInfoDto);
        this.currentDatabaseInfo(dbInfo);
        
        dbInfo.dynamicNodesDistribution.subscribe((dynamic) => {
            new toggleDynamicNodeAssignmentCommand(this.activeDatabase().name, dynamic)
                .execute();
        });
    }

    addNode() {
        if (this.currentDatabaseInfo().isEncrypted()) {
            const addKeyView = new defineNodeEncryptionKey(this.activeDatabase().name, this.selectedClusterNode());
            
            app.showBootstrapDialog(addKeyView)
                .done(result => {
                    if (result) {
                        this.addNodeInternal();
                    }
                });
        } else {
            this.addNodeInternal();
        }
    }
    
    addNodeInternal() {
        this.spinners.addNode(true);

        new addNodeToDatabaseGroupCommand(this.activeDatabase().name, this.selectedClusterNode())
            .execute()
            .done(() => {
                this.selectedClusterNode(null);
            })
            .always(() => this.spinners.addNode(false));
    }

    deleteNodeFromGroup(node: databaseGroupNode, hardDelete: boolean) {
        const db = this.activeDatabase();
        const nodeTag = node.tag();
        this.confirmationMessage("Are you sure", "Do you want to delete database '" + this.activeDatabase().name + "' from node: " + node.tag() + "?", ["Cancel", "Yes, delete"])
            .done(result => {
                if (result.can) {
                    new deleteDatabaseFromNodeCommand(db, [nodeTag], hardDelete)
                        .execute();
                }
            });
    }

    showErrorDetails(tag: string) {
        const node = this.nodes().find(x => x.tag() === tag);

        app.showBootstrapDialog(new showDataDialog("Error details. Node: " + tag, node.lastError(), "plain"));
    }

}

export = manageDatabaseGroup;
