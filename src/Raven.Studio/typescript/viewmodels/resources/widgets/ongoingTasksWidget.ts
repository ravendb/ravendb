import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");

class taskNodes {
    nodes = ko.observable<Set<string>>();
    nodesArray: KnockoutComputed<string[]>;

    constructor() {
        this.nodes(new Set<string>());
        this.nodesArray = ko.pureComputed(() => _.sortBy(Array.from(this.nodes())));
    }

    addNode(nodeTag: string): void {
        const nodeSet = this.nodes();
        nodeSet.add(nodeTag);
        this.nodes(nodeSet);
    }

    removeNode(nodeTag: string): void {
        const nodeSet = this.nodes();
        nodeSet.delete(nodeTag);
        this.nodes(nodeSet);
    }
}

// inner table item
class databaseTaskItem {
    databaseName = ko.observable<string>();
    taskCount = ko.observable<number>();
    taskNodes = ko.observable<taskNodes>();
    
    constructor() {
        this.taskNodes(new taskNodes());
    }
}

// parent table item
class ongoingTaskItem {
    taskName = ko.observable<string>();
    taskCount = ko.observable<number>();
    taskNodes = ko.observable<taskNodes>();
    
    innerItems = ko.observableArray<databaseTaskItem>([]);

    typeClass = ko.observable<string>();
    iconClass = ko.observable<string>();

    constructor(name: string, count: number, typeClass: string, iconClass: string) {
        this.taskName(name);
        this.taskCount(count);
        this.taskNodes(new taskNodes());
        
        this.typeClass(typeClass);
        this.iconClass(iconClass);
    }

    getTaskNameForUI(): TasksNamesInUI {
        switch (this.taskName()) {
            case "ExternalReplicationCount": return "External Replication";
            case "ReplicationHubCount": return "Replication Hub";
            case "ReplicationSinkCount": return "Replication Sink";
            case "RavenEtlCount": return "RavenDB ETL";
            case "OlapEtlCount": return "OLAP ETL";
            case "SqlEtlCount": return "SQL ETL";
            case "ElasticSearchEtlCount": return "Elasticsearch ETL";
            case "PeriodicBackupCount": return "Backup";
            case "SubscriptionCount": return "Subscription";
        }
    }
}

class nodeRawData {
    nodeTag = ko.observable<string>();
    databaseItems = ko.observableArray<Raven.Server.Dashboard.DatabaseOngoingTasksInfoItem>([]);
}

class ongoingTasksWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.OngoingTasksPayload> {

    view = require("views/resources/widgets/ongoingTasksWidget.html");
    
    allNodesData = ko.observableArray<nodeRawData>();
    
    taskList = ko.observableArray<ongoingTaskItem>([]);

    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "OngoingTasks";
    }

    constructor(controller: clusterDashboard) {
        super(controller);
        
        this.taskList([
            new ongoingTaskItem("ExternalReplicationCount", 0, "external-replication", "icon-external-replication"),
            new ongoingTaskItem("ReplicationHubCount", 0, "replication-hub", "icon-pull-replication-hub"),
            new ongoingTaskItem("ReplicationSinkCount", 0, "replication-sink", "icon-pull-replication-agent"),
            new ongoingTaskItem("RavenEtlCount", 0, "ravendb-etl", "icon-ravendb-etl"),
            new ongoingTaskItem("OlapEtlCount", 0, "olap-etl", "icon-olap-etl"),
            new ongoingTaskItem("SqlEtlCount", 0, "sql-etl", "icon-sql-etl"),
            new ongoingTaskItem("ElasticSearchEtlCount", 0, "elastic-etl", "icon-elastic-search-etl"),
            new ongoingTaskItem("PeriodicBackupCount", 0, "periodic-backup", "icon-backups"),
            new ongoingTaskItem("SubscriptionCount", 0, "subscription", "icon-subscription")
        ]);
    }

    compositionComplete() {
        super.compositionComplete();
        this.enableSyncUpdates();

        for (let ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }
    }

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.OngoingTasksPayload) {
        
        const nodeToUpdate = this.allNodesData().find(node => node.nodeTag() === nodeTag);

        if (nodeToUpdate) {
            data.Items.forEach(dbInfo => {
                const filteredArray = nodeToUpdate.databaseItems().filter(x => x.Database !== dbInfo.Database);
                filteredArray.push(dbInfo);
                nodeToUpdate.databaseItems(filteredArray);
            });
        } else {
            const nodeToUpdate = new nodeRawData();
            nodeToUpdate.nodeTag(nodeTag);
            data.Items.forEach(dbInfo => nodeToUpdate.databaseItems().push(dbInfo));
            this.allNodesData().push(nodeToUpdate);
        }
        
        // at this point allNodesData is updated - now update UI classes
        this.initTaskList();
        
        this.allNodesData().forEach(node => {
            for (let i = 0; i < this.taskList().length; i++) {
                this.sumTaskCount(node, this.taskList()[i])
            }
        });
    }
    
    private sumTaskCount(node: nodeRawData, taskItem: ongoingTaskItem): void {
        let mustAddNode = false;
        
        node.databaseItems().forEach(dbItem => {
            const countToAdd = dbItem[taskItem.taskName() as keyof Raven.Server.Dashboard.DatabaseOngoingTasksInfoItem] as number;
            const totalCount = taskItem.taskCount();
            
            taskItem.taskCount(totalCount + countToAdd);
            mustAddNode = mustAddNode || countToAdd > 0;

            this.manageInnerItems(taskItem, dbItem.Database, countToAdd, node.nodeTag());
        });
        
        this.manageParentItemNodes(taskItem, node.nodeTag(), mustAddNode);
    }
    
    private manageInnerItems(taskItem: ongoingTaskItem, database: string, countToAdd: number, nodeTag: string): void {
        // find relevant inner item and update
        // TODO: this area is not fully checked - need to debug when adding databases details to the Tasks Panel - RavenDB-18161
        
        const innerItems = taskItem.innerItems();
        const databaseToUpdate = innerItems.find(db => db.databaseName() === database);
        
        if (databaseToUpdate) {
            if (countToAdd > 0) {
                const totalCount = databaseToUpdate.taskCount();
                databaseToUpdate.taskCount(totalCount + countToAdd);
                databaseToUpdate.taskNodes().addNode(nodeTag);
            } else {
                databaseToUpdate.taskNodes().removeNode(nodeTag);
                if (databaseToUpdate.taskCount() === 0) {
                    const filteredArray = innerItems.filter(db => db.databaseName() !== database);
                    taskItem.innerItems(filteredArray);
                }
            }
        } else {
            const databaseToUpdate = new databaseTaskItem();
            databaseToUpdate.databaseName(database);
            databaseToUpdate.taskCount(countToAdd);
            databaseToUpdate.taskNodes().addNode(nodeTag)
            taskItem.innerItems().push(databaseToUpdate);
        }
    }
    
    private manageParentItemNodes(taskItem: ongoingTaskItem, tag: string, mustAddNode: boolean): void {
        if (mustAddNode) {
            taskItem.taskNodes().addNode(tag)
        } else {
            taskItem.taskNodes().removeNode(tag)
        }
    }
    
    private initTaskList(): void {
        this.taskList().forEach(task => {
            task.taskCount(0);
            task.taskNodes(new taskNodes());
            task.innerItems([]);
        });
    }

    getNodeClass(nodeTag: string): string {
        return `node-label node-${nodeTag}`;
    }
}

export = ongoingTasksWidget;
