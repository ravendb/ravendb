import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import iconsPlusTextColumn = require("widgets/virtualGrid/columns/iconsPlusTextColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import genUtils = require("common/generalUtils");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

class rawTaskItem {
    constructor(public type: string, public count: number, public dbName: string, public node: string) {
    }
}

class taskItem {
    taskType = ko.observable<string>();
    taskCount = ko.observable<number>();
    taskNodes = ko.observableArray<string>();
    
    databaseName = ko.observable<string>();

    isTitleItem = ko.observable<boolean>(false);
    even: boolean = false;

    constructor(type: string, count: number, dbName?: string, nodes?: string[]) {
        this.taskType(type);
        this.databaseName(dbName);
        this.taskCount(count);
        this.taskNodes(nodes);
        
        if (!dbName) {
            this.isTitleItem(true);
        }
    }

    updateWith(countToAdd: number, nodeToAdd: string) {
        this.taskCount(this.taskCount() + countToAdd);

        const foundTag = this.taskNodes().find(x => x === nodeToAdd);
        if (!foundTag) {
            this.taskNodes.push(nodeToAdd);
        }
    }
    
    static itemFromRaw(rawItem: rawTaskItem): taskItem {
        return new taskItem(rawItem.type, rawItem.count, rawItem.dbName, [rawItem.node]);
    }
}

interface taskInfo {
    nameForUI: string,
    icon: string,
    colorClass: string
}

class ongoingTasksWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.OngoingTasksPayload> {

    static readonly taskInfoRecord: Record<string, taskInfo> = {
        "ExternalReplication": {
            nameForUI: "External Replication",
            icon: "icon-external-replication",
            colorClass: "external-replication"
        },
        "ReplicationHub": {
            nameForUI: "Replication Hub",
            icon: "icon-pull-replication-hub",
            colorClass: "replication-hub"
        },
        "ReplicationSink": {
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
        "PeriodicBackup": {
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
        
        grid.init(() => this.getGridData(), (containerWidth, results) => this.prepareColumns(containerWidth, results));

        this.enableSyncUpdates();

        for (let ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }
    }

    protected afterSyncUpdate(updatesCount: number) {
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
    
    private getIconPlusTextHtml(item: taskItem): iconPlusText[] {
        const name = ongoingTasksWidget.taskInfoRecord[item.taskType()].nameForUI;
        
        return [{
            title: name + " task",
            text: name,
            iconClass: ongoingTasksWidget.taskInfoRecord[item.taskType()].icon,
            textClass: ongoingTasksWidget.taskInfoRecord[item.taskType()].colorClass
        }];
    }

    private getTaskCountText(x: taskItem): string {
        const count = x.taskCount();
        if (x.isTitleItem()) {
            return x.taskCount() ? `Total = ${x.taskCount().toLocaleString()}` : "x";
        } else {
            return x.taskCount().toLocaleString();
        }
    }
    
    private prepareColumns(containerWidth: number, results: pagedResult<taskItem>): virtualColumn[] {
        const grid = this.gridController();
        return [
            new iconsPlusTextColumn<taskItem>(grid, x => x.isTitleItem() ? this.getIconPlusTextHtml(x) : "", "Task", "30%"),
            
            new textColumn<taskItem>(grid, x => x.isTitleItem() ? "" : x.databaseName(), "Database", "30%"),

            new textColumn<taskItem>(grid, x => this.getTaskCountText(x), "Count", "15%"),

            new textColumn<taskItem>(grid, x => x.isTitleItem() ? "" : x.taskNodes().toString(), "Nodes", "20%") // TODO - work on node tag column
        ];
    }

    reducePerDatabase(itemsArray: rawTaskItem[]): taskItem[] {
        const output: taskItem[] = [];

        for (let rawItem of itemsArray) {
            const existingItem = output.find(x => (x.databaseName() === rawItem.dbName))

            if (existingItem) {
                existingItem.updateWith(rawItem.count, rawItem.node);
            } else {
                output.push(taskItem.itemFromRaw(rawItem));
            }
        }

        return output;
    }
    
    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.OngoingTasksPayload) {
        this.spinners.loading(false);
            
        // 1. update raw data
        const rawDataWithoutIncomingNode = this.rawData().filter(x => x.node !== nodeTag);
        const tempRawData = rawDataWithoutIncomingNode;

        data.Items.forEach(x => {
            for (let key in x) {
                if (!x.hasOwnProperty(key))
                    continue;

                const value = (x as any)[key];
                if (key !== "Database" && value > 0) {
                    const newItem = new rawTaskItem(key.replace("Count", ""), value, genUtils.escapeHtml(x.Database), nodeTag);
                    tempRawData.push(newItem);
                }
            }
        });

        this.rawData(tempRawData);

        // 2. create the data to show
        const tempDataToShow: Array<taskItem> = [];

        for (let taskType in ongoingTasksWidget.taskInfoRecord) {
            const filteredItemsByType = this.rawData().filter(x => x.type === taskType);
            
            const reducedItems = this.reducePerDatabase(filteredItemsByType);

            if (reducedItems && reducedItems.length) {
                reducedItems.sort((a: taskItem, b: taskItem) => genUtils.sortAlphaNumeric(a.databaseName(), b.databaseName()));
                reducedItems.map(x => x.taskNodes().sort((a: string, b: string) => genUtils.sortAlphaNumeric(a, b)));
            }

            const totalTasksPerType = reducedItems.reduce((sum, item) => sum + item.taskCount(), 0);
            const titleItem = new taskItem(taskType, totalTasksPerType);

            tempDataToShow.push(titleItem);

            if (reducedItems && reducedItems.length) {
                tempDataToShow.push(...reducedItems);
            }

            this.dataToShow(tempDataToShow);
        }
    }

    getNodeClass(nodeTag: string): string {
        return `node-label node-${nodeTag}`;
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
