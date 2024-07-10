import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import iconsPlusTextColumn = require("widgets/virtualGrid/columns/iconsPlusTextColumn");
import genUtils = require("common/generalUtils");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import multiNodeTagsColumn = require("widgets/virtualGrid/columns/multiNodeTagsColumn");
import taskItem = require("models/resources/widgets/taskItem");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import appUrl = require("common/appUrl");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class ongoingTasksWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.OngoingTasksPayload> {

    view = require("views/resources/widgets/ongoingTasksWidget.html");
    
    static readonly taskInfoRecord: Record<StudioTaskType, taskInfo> = {
        "Replication": {
            nameForUI: "External Replication",
            icon: "icon-external-replication",
            colorClass: "external-replication"
        },
        "PullReplicationAsHub": {
            nameForUI: "Replication Hub",
            icon: "icon-pull-replication-hub",
            colorClass: "replication-hub"
        },
        "PullReplicationAsSink": {
            nameForUI: "Replication Sink",
            icon: "icon-pull-replication-agent",
            colorClass: "replication-sink"
        },
        "RavenEtl": {
            nameForUI: "RavenDB ETL",
            icon: "icon-ravendb-etl",
            colorClass: "ravendb-etl"
        },
        "OlapEtl": {
            nameForUI: "OLAP ETL",
            icon: "icon-olap-etl",
            colorClass: "olap-etl"
        },
        "SqlEtl": {
            nameForUI: "SQL ETL",
            icon: "icon-sql-etl",
            colorClass: "sql-etl"
        },
        "ElasticSearchEtl": {
            nameForUI: "Elasticsearch ETL",
            icon: "icon-elastic-search-etl",
            colorClass: "elastic-etl"
        },
        "KafkaQueueEtl": {
            nameForUI: "Kafka ETL",
            icon: "icon-kafka-etl",
            colorClass: "kafka-etl"
        },
        "RabbitQueueEtl": {
            nameForUI: "RabbitMQ ETL",
            icon: "icon-rabbitmq-etl",
            colorClass: "rabbitmq-etl"
        },
        "Backup": {
            nameForUI: "Backup",
            icon: "icon-backups",
            colorClass: "periodic-backup"
        },
        "Subscription": {
            nameForUI: "Subscription",
            icon: "icon-subscription",
            colorClass: "subscription"
        }
    }

    protected gridController = ko.observable<virtualGridController<taskItem>>();
    
    rawData = ko.observableArray<rawTaskItem>([]);
    dataToShow = ko.observableArray<taskItem>([]);

    spinners = {
        loading: ko.observable<boolean>(true)
    }

    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "OngoingTasks";
    }

    constructor(controller: clusterDashboard) {
        super(controller);
}

    compositionComplete() {
        super.compositionComplete();
    
        const grid = this.gridController();
        grid.headerVisible(true);
        
        grid.customRowClassProvider(item => item.even ? ["even"] : []);
        
        grid.init(() => this.getGridData(), () => this.prepareColumns());

        this.enableSyncUpdates();

        for (const ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
    }
}

    protected afterSyncUpdate() {
        this.gridController().reset(false);
    }
    
    afterComponentResized() {
        super.afterComponentResized();
        this.gridController().reset(true, true);
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.gridController().reset(false);
    }
        
    private getGridData(): JQueryPromise<pagedResult<taskItem>> {
        const items = this.dataToShow();

        this.applyPerDatabaseStripes(items);
        
        return $.when({
            totalResultCount: items.length,
            items
        });
    }

    private getTaskTypeHtml(item: taskItem): iconPlusText[] {
        const name = ongoingTasksWidget.taskInfoRecord[item.taskType()].nameForUI;
        
        return [{
            title: name + " task",
            text: name,
            iconClass: ongoingTasksWidget.taskInfoRecord[item.taskType()].icon,
            textClass: ongoingTasksWidget.taskInfoRecord[item.taskType()].colorClass
        }];
        }

    private getTaskCountHtml(item: taskItem): iconPlusText[] {
        return [{
            title: this.getTaskCountTitle(item),
            text: this.getTaskCountText(item),
            iconClass: this.getTaskCountIcon(item),
            textClass: this.getTaskCountClass(item)
        }];
    }

    private getTaskCountTitle(item: taskItem): string {
        if (item.isTitleItem()) {
            const count = item.taskCount();
            const taskPart = count > 1 ? "tasks" : "task";

            return count > 0 ? `Total of ${count} ${item.taskType()} ${taskPart}` : "";
}

        return "";
}

    private getTaskCountText(item: taskItem): string {
        const count = item.taskCount();

        if (item.isTitleItem() && !item.taskCount()) {
            return "";
        } 
    
        return count.toLocaleString();
    }
    
    private getTaskCountIcon(item: taskItem): string {
        if (item.isTitleItem() && !item.taskCount()) {
            return "icon-cancel";
    }

        return "";
    }

    private getTaskCountClass(item: taskItem): string {
        if (!item.isTitleItem()) {
            return ""
        }
        
        if (item.taskCount()) {
            return "text-bold";
        }

        return "text-muted small";
    }

    private getOngoingTasksUrlForNode(databaseName: string, nodeTag: string): string {
        const node = clusterTopologyManager.default.getClusterNodeByTag(nodeTag);

        if (!node) {
            return "#"
        }

        return node.serverUrl() + "/studio/index.html" + appUrl.forOngoingTasks(databaseName);
    }

    private prepareColumns(): virtualColumn[] {
        const grid = this.gridController();
        return [
            new iconsPlusTextColumn<taskItem>(
                grid, x => x.isTitleItem() ? this.getTaskTypeHtml(x) : "",
                "Task",
                "30%",
                {
                    headerTitle: "Tasks type"
                }
            ),
            new iconsPlusTextColumn<taskItem>(
                grid,
                x => this.getTaskCountHtml(x),
                "Count",
                "15%",
                {
                    headerTitle: "Tasks count"
                }
            ),
            new hyperlinkColumn<taskItem>(
                grid, x => x.isTitleItem() ? "" : x.databaseName(),
                x => appUrl.forOngoingTasks(x.databaseName()),
                "Database",
                "30%",
                {
                    title: x => `Ongoing tasks for database ${x.databaseName()}`
                }
            ),
            new multiNodeTagsColumn<taskItem>(
                grid,
                taskItem.createNodeTagsProvider(),
                "20%",
                {
                    headerTitle: "Nodes running the tasks",
                    nodeHrefAccessor: (item, nodeTag) => this.getOngoingTasksUrlForNode(item.databaseName(), nodeTag),
                    nodeLinkTitleAccessor: (item, nodeTag) => `Ongoing tasks for database ${item.databaseName()} on node ${nodeTag}`
                }
            )
        ];
    }

    reducePerDatabase(itemsArray: rawTaskItem[]): taskItem[] {
        const output: taskItem[] = [];
        
        for (const rawItem of itemsArray) {
            const existingItem = output.find(x => x.databaseName() === rawItem.dbName)

            if (existingItem) {
                existingItem.updateWith(rawItem.count, rawItem.node);
        } else {
                output.push(taskItem.itemFromRaw(rawItem));
        }
        }
        
        return output;
    }
        
    private getTaskType(input: string): StudioTaskType {
        switch (input) {
            case "ExternalReplicationCount":
                return "Replication";
            case "ReplicationHubCount":
                return "PullReplicationAsHub";
            case "ReplicationSinkCount":
                return "PullReplicationAsSink";
            case "RavenEtlCount":
                return "RavenEtl";
            case "SqlEtlCount":
                return "SqlEtl";
            case "OlapEtlCount":
                return "OlapEtl";
            case "ElasticSearchEtlCount":
                return "ElasticSearchEtl";
            case "KafkaEtlCount":
                return "KafkaQueueEtl";
            case "RabbitMqEtlCount":
                return "RabbitQueueEtl";
            case "PeriodicBackupCount":
                return "Backup";
            case "SubscriptionCount":
                return "Subscription";
            default:
                throw new Error("Unknown task type count received:" + input);
            }
    }
    
    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.OngoingTasksPayload) {
        this.spinners.loading(false);
        
        // 1. update raw data
        const rawDataWithoutIncomingNode = this.rawData().filter(x => x.node !== nodeTag);
        const tempRawData = rawDataWithoutIncomingNode;
            
        data.Items.forEach(x => {
            for (const key in x) {
                // eslint-disable-next-line no-prototype-builtins
                if (!x.hasOwnProperty(key))
                    continue;

                const value = (x as any)[key];
                
                if (key !== "Database" && value > 0) {
                    const taskType = this.getTaskType(key);
                    
                    tempRawData.push({
                        type: taskType,
                        count: value,
                        dbName: genUtils.escapeHtml(x.Database),
                        node: nodeTag
        });
                }
            }
        });
        
        this.rawData(tempRawData);

        // 2. create the data to show
        const tempDataToShow: Array<taskItem> = [];

        for (const taskType in ongoingTasksWidget.taskInfoRecord) {
            const filteredItemsByType = this.rawData().filter(x => x.type === taskType);
            
            const reducedItems = this.reducePerDatabase(filteredItemsByType);

            if (reducedItems && reducedItems.length) {
                reducedItems.sort((a: taskItem, b: taskItem) => genUtils.sortAlphaNumeric(a.databaseName(), b.databaseName()));
                reducedItems.map(x => x.nodeTags().sort((a: string, b: string) => genUtils.sortAlphaNumeric(a, b)));
    }
    
            const totalTasksPerType = reducedItems.reduce((sum, item) => sum + item.taskCount(), 0);
            const titleItem = new taskItem(taskType as StudioTaskType, totalTasksPerType);
        
            tempDataToShow.push(titleItem);
        
            if (reducedItems && reducedItems.length) {
                tempDataToShow.push(...reducedItems);
                }

            this.dataToShow(tempDataToShow);
            }
        }
    
    protected applyPerDatabaseStripes(items: taskItem[]) {
        // TODO: RavenDB-17013 - stripes not working correctly after scroll

        for (let i = 0; i < items.length; i++) {
            const item = items[i];

            if (item.isTitleItem()) {
                item.even = true;
        } else {
                item.even = false;
        }
    }
    }
    }

export = ongoingTasksWidget;
