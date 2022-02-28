import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

enum taskName {
    ExternalReplication = 0,
    ReplicationHub,
    ReplicationSink,
    RavenEtl,
    OlapEtl,
    SqlEtl,
    Backup,
    Subscription
}

class ongoingTaskItem {
    protected clusterManager = clusterTopologyManager.default;
    
    taskName = ko.observable<taskName>();
    taskCount = ko.observable<number>();
    
    taskNodes = ko.observable<Set<string>>();
    taskNodesArray: KnockoutComputed<string[]>
    
    typeClass = ko.observable<string>();
    iconClass = ko.observable<string>();

    constructor(name: taskName, count: number, typeClass: string, iconClass: string) {
        this.taskName(name);
        this.taskCount(count);
        this.taskNodes(new Set<string>());
        
        this.typeClass(typeClass);
        this.iconClass(iconClass);
        
        this.taskNodesArray = ko.pureComputed(() => {
            return _.sortBy(Array.from(this.taskNodes()));
        })
    }
    
    getNodeClass(nodeTag: string): string {
        return `node-label node-${nodeTag}`;
    }

    getTaskNameForUI(): TasksNamesInUI {
        switch (this.taskName()) {
            case taskName.ExternalReplication: return "External Replication";
            case taskName.ReplicationHub: return "Replication Hub";
            case taskName.ReplicationSink: return "Replication Sink";
            case taskName.RavenEtl: return "RavenDB ETL";
            case taskName.OlapEtl: return "OLAP ETL";
            case taskName.SqlEtl: return "SQL ETL";
            case taskName.Backup: return "Backup";
            case taskName.Subscription: return "Subscription";
        }
    }
}

class ongoingTasksWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.OngoingTasksPayload> {

    nodeTagToTasksCount = ko.observable<dictionary<Raven.Server.Dashboard.DatabaseOngoingTasksInfoItem>>({});
    
    taskList = ko.observableArray<ongoingTaskItem>([]);

    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "OngoingTasks";
    }

    constructor(controller: clusterDashboard) {
        super(controller);

        const tasks = this.taskList();
        tasks[taskName.ExternalReplication] = new ongoingTaskItem(taskName.ExternalReplication, 0, "external-replication", "icon-external-replication");
        tasks[taskName.ReplicationHub] = new ongoingTaskItem(taskName.ReplicationHub, 0, "replication-hub", "icon-pull-replication-hub");
        tasks[taskName.ReplicationSink] = new ongoingTaskItem(taskName.ReplicationSink, 0, "replication-sink", "icon-pull-replication-agent");
        tasks[taskName.RavenEtl] = new ongoingTaskItem(taskName.RavenEtl, 0, "ravendb-etl", "icon-ravendb-etl");
        tasks[taskName.OlapEtl] = new ongoingTaskItem(taskName.OlapEtl, 0, "olap-etl", "icon-olap-etl");
        tasks[taskName.SqlEtl] = new ongoingTaskItem(taskName.SqlEtl, 0, "sql-etl", "icon-sql-etl");
        tasks[taskName.Backup] = new ongoingTaskItem(taskName.Backup, 0, "periodic-backup", "icon-backups");
        tasks[taskName.Subscription] = new ongoingTaskItem(taskName.Subscription, 0, "subscription", "icon-subscription");
    }

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.OngoingTasksPayload) {
        const item = data.Items[0]; // has aggregated data for ALL databases for the node

        const tagsToCount = this.nodeTagToTasksCount();

        tagsToCount[nodeTag] = {
            ExternalReplicationCount: item.ExternalReplicationCount,
            ReplicationHubCount: item.ReplicationHubCount,
            ReplicationSinkCount: item.ReplicationSinkCount,
            RavenEtlCount: item.RavenEtlCount,
            OlapEtlCount: item.OlapEtlCount,
            SqlEtlCount: item.SqlEtlCount,
            PeriodicBackupCount: item.PeriodicBackupCount,
            SubscriptionCount:item.SubscriptionCount,
            Database: "All"
        };

        const ongoingTasksCountArray = Object.keys(tagsToCount).map(k => tagsToCount[k]);

        const totalExtRep = ongoingTasksCountArray.map(x => x.ExternalReplicationCount).reduce((a,b) => { return a + b });
        const totalHub = ongoingTasksCountArray.map(x => x.ReplicationHubCount).reduce((a,b) => { return a + b });
        const totalSink = ongoingTasksCountArray.map(x => x.ReplicationSinkCount).reduce((a,b) => { return a + b });
        const totalRavenEtl = ongoingTasksCountArray.map(x => x.RavenEtlCount).reduce((a,b) => { return a + b });
        const totalOlapEtl = ongoingTasksCountArray.map(x => x.OlapEtlCount).reduce((a,b) => { return a + b });
        const totalSqlEtl = ongoingTasksCountArray.map(x => x.SqlEtlCount).reduce((a,b) => { return a + b });
        const totalBackup = ongoingTasksCountArray.map(x => x.PeriodicBackupCount).reduce((a,b) => { return a + b });
        const totalSubscription = ongoingTasksCountArray.map(x => x.SubscriptionCount).reduce((a,b) => { return a + b });

        const tasks = this.taskList();
        tasks[taskName.ExternalReplication].taskCount(totalExtRep);
        tasks[taskName.ReplicationHub].taskCount(totalHub);
        tasks[taskName.ReplicationSink].taskCount(totalSink);
        tasks[taskName.RavenEtl].taskCount(totalRavenEtl);
        tasks[taskName.OlapEtl].taskCount(totalOlapEtl);
        tasks[taskName.SqlEtl].taskCount(totalSqlEtl);
        tasks[taskName.Backup].taskCount(totalBackup);
        tasks[taskName.Subscription].taskCount(totalSubscription);

        for (const key in tagsToCount) {
            if (tagsToCount[key].ExternalReplicationCount) {
                this.addNodeTag(key, taskName.ExternalReplication);
            } else {
                this.removeNodeTag(key, taskName.ExternalReplication);
            }
            if (tagsToCount[key].ReplicationHubCount) {
                this.addNodeTag(key, taskName.ReplicationHub);
            } else {
                this.removeNodeTag(key, taskName.ReplicationHub);
            }
            if (tagsToCount[key].ReplicationSinkCount) {
                this.addNodeTag(key, taskName.ReplicationSink);
            } else {
                this.removeNodeTag(key, taskName.ReplicationSink);
            }
            if (tagsToCount[key].RavenEtlCount) {
                this.addNodeTag(key, taskName.RavenEtl);
            } else {
                this.removeNodeTag(key, taskName.RavenEtl);
            }
            if (tagsToCount[key].OlapEtlCount) {
                this.addNodeTag(key, taskName.OlapEtl);
            } else {
                this.removeNodeTag(key, taskName.OlapEtl);
            }
            if (tagsToCount[key].SqlEtlCount) {
                this.addNodeTag(key, taskName.SqlEtl);
            } else {
                this.removeNodeTag(key, taskName.SqlEtl);
            }
            if (tagsToCount[key].PeriodicBackupCount) {
                this.addNodeTag(key, taskName.Backup);
            } else {
                this.removeNodeTag(key, taskName.Backup);
            }
            if (tagsToCount[key].SubscriptionCount) {
                this.addNodeTag(key, taskName.Subscription);
            } else {
                this.removeNodeTag(key, taskName.Subscription);
            }
        }
    }
    
    private addNodeTag (nodeTag: string, name: taskName): void {
        const nodeSet = this.taskList()[name].taskNodes();
        nodeSet.add(nodeTag);
        this.taskList()[name].taskNodes(nodeSet);
    }
    
    private removeNodeTag(nodeTag: string, name: taskName): void {
        this.taskList()[name].taskNodes().delete(nodeTag);
    }
}

export = ongoingTasksWidget;
