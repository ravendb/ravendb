import viewModelBase = require("viewmodels/viewModelBase");

import app = require("durandal/app");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import databaseInfo = require("models/resources/info/databaseInfo");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

import databaseGroupNode = require("models/resources/info/databaseGroupNode");
import deleteDatabaseFromNodeCommand = require("commands/resources/deleteDatabaseFromNodeCommand");
import databaseGroupGraph = require("models/database/dbGroup/databaseGroupGraph");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import showDataDialog = require("viewmodels/common/showDataDialog");
import addNewNodeToDatabaseGroup = require("viewmodels/resources/addNewNodeToDatabaseGroup");
import reorderNodesInDatabaseGroupCommand = require("commands/database/dbGroup/reorderNodesInDatabaseGroupCommand");
import license = require("models/auth/licenseModel");
import eventsCollector = require("common/eventsCollector");
import messagePublisher = require("common/messagePublisher");
import generalUtils = require("common/generalUtils");
import toggleDynamicNodeAssignmentCommand = require("commands/database/dbGroup/toggleDynamicNodeAssignmentCommand");
import jsonUtil = require("common/jsonUtil");

class manageDatabaseGroup extends viewModelBase {

    dynamicDatabaseDistribution = ko.observable<boolean>(false);
    nodes = ko.observableArray<databaseGroupNode>([]);
    deletionInProgress = ko.observableArray<string>([]);
    isEncrypted = ko.observable<boolean>(false);
    
    priorityOrder = ko.observableArray<string>([]);
    
    fixOrder = ko.observable<boolean>(false);
    
    clearNodesList = ko.observable<boolean>(false);
    
    selectedClusterNode = ko.observable<string>();
    
    inSortableMode = ko.observable<boolean>(false);
    private sortable: any;

    private graph = new databaseGroupGraph();

    nodeTag = clusterTopologyManager.default.localNodeTag;
    addNodeEnabled: KnockoutComputed<boolean>;
    showDynamicDatabaseDistributionWarning: KnockoutComputed<boolean>;
    
    anyNodeHasError: KnockoutComputed<boolean>;

    dirtyFlag: () => DirtyFlag;

    constructor() {
        super();

        this.bindToCurrentInstance("addNode", "deleteNodeFromGroup", "showErrorDetails");

        this.initObservables();

        this.dirtyFlag = new ko.DirtyFlag([
            this.fixOrder,
            this.nodes
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initObservables() {
        
        this.anyNodeHasError = ko.pureComputed(() => {
            if (clusterTopologyManager.default.votingInProgress()) {
                return true;
            }
            
            const topology = clusterTopologyManager.default.topology();
            
            if (!topology) {
                return true;
            }
            
            const nodes = topology.nodes();
            
            let allConnected = true;
            
            for (let i = 0; i < nodes.length; i++) {
                if (!nodes[i].connected()) {
                    allConnected = false;
                }
            }
            
            return !allConnected;
        });
        
        this.addNodeEnabled = ko.pureComputed(() => {
            const tags = clusterTopologyManager.default.topology().nodes().map(x => x.tag());
            const existingTags = this.nodes().map(x => x.tag());
            return _.without(tags, ...existingTags).length > 0;
        });

        this.showDynamicDatabaseDistributionWarning = ko.pureComputed(() => {
            return !license.licenseStatus().HasDynamicNodesDistribution;
        });
        
        this.registerDisposable(
            this.anyNodeHasError.subscribe((error) => {
                if (error && this.inSortableMode()) {
                    messagePublisher.reportWarning("Can't reorder nodes, when at least one node is down or voting is in progress.");
                    this.cancelReorder();
                }
            }));
    }

    activate(args: any) {
        super.activate(args);
        
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
    }
    
    attached() {
        super.attached();

        this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications()
            .watchAllDatabaseChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.refresh()));
    }

    compositionComplete(): void {
        super.compositionComplete();

        this.registerDisposableHandler($(document), "fullscreenchange", () => {
            $("body").toggleClass("fullscreen", $(document).fullScreen());
            this.graph.onResize();
        });

        this.dynamicDatabaseDistribution.subscribe(dynamic => {
            new toggleDynamicNodeAssignmentCommand(this.activeDatabase().name, dynamic)
                .execute();
        });

        this.graph.init($("#databaseGroupGraphContainer"));
    }
    
    deactivate() {
        super.deactivate();
        if (this.sortable) {
            this.sortable.destroy();
        }
    }

    enableNodesSort() {
        this.inSortableMode(true);

        const list = $(".nodes-list .not-deleted-nodes")[0];

        this.sortable = new Sortable(list,
            {
                onEnd: (event: { oldIndex: number, newIndex: number }) => {
                    const nodes = this.nodes();
                    nodes.splice(event.newIndex, 0, nodes.splice(event.oldIndex, 1)[0]);
                    this.nodes(nodes);
                }
            });
        
        this.dirtyFlag().reset();
    }

    cancelReorder() {
        this.disableNodesSort();
    }

    saveNewOrder() {
        eventsCollector.default.reportEvent("db-group", "save-order");
        const newOrder = this.nodes().map(x => x.tag());
        
        new reorderNodesInDatabaseGroupCommand(this.activeDatabase().name, newOrder, this.fixOrder())
            .execute()
            .done(() => {
                this.disableNodesSort();
            });
    }
    
    private disableNodesSort() {
        this.inSortableMode(false);
        
        if (this.sortable) {
            this.sortable.destroy();
            this.sortable = null;
        }

        // hack: force list to be empty - sortable (RubaXa version) doesn't play well with ko:foreach
        // https://github.com/RubaXa/Sortable/issues/533
        this.clearNodesList(true);
        
        // fetch fresh copy
        this.refresh();
    }
    
    private refresh() {
        if (!this.inSortableMode()) {
            $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
        }
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
        const incomingDbInfo = new databaseInfo(dbInfoDto);
        
        if (this.clearNodesList()) {
            $(".nodes-list .not-deleted-nodes").empty();
            this.nodes([]);
            this.clearNodesList(false);
        }
        
        this.updateNodes(incomingDbInfo.nodes());
        this.deletionInProgress(incomingDbInfo.deletionInProgress());
        this.isEncrypted(incomingDbInfo.isEncrypted());
        this.dynamicDatabaseDistribution(incomingDbInfo.dynamicDatabaseDistribution());
        this.priorityOrder(incomingDbInfo.priorityOrder());
        this.fixOrder(incomingDbInfo.priorityOrder() && incomingDbInfo.priorityOrder().length > 0);
    }
    
    private updateNodes(incomingData: databaseGroupNode[]) {
        const local = this.nodes();
        
        const localTags = local.map(x => x.tag());
        const remoteTags = incomingData.map(x => x.tag());
        
        if (_.isEqual(localTags, remoteTags)) {
            // we have same node tags: do in place update
         
            incomingData.forEach(d => {
                local.find(x => x.tag() === d.tag()).update(d);
            });
        } else {
            // node tags changed
            this.nodes(incomingData);
        }
    }

    addNode() {
        const addKeyView = new addNewNodeToDatabaseGroup(this.activeDatabase().name, this.nodes(), this.isEncrypted());
        app.showBootstrapDialog(addKeyView);
    }
    
    deleteNodeFromGroup(node: databaseGroupNode, hardDelete: boolean) {
        const db = this.activeDatabase();
        const nodeTag = node.tag();
        this.confirmationMessage("Are you sure", "Do you want to delete database '" + generalUtils.escapeHtml(this.activeDatabase().name) + "' from node: " + node.tag() + "?", {
            buttons: ["Cancel", "Yes, delete"],
            html: true
        })
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
