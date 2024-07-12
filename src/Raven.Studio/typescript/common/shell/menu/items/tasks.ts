import { bridgeToReact } from "common/reactUtils";
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import { BackupsPage } from "components/pages/database/tasks/backups/BackupsPage";
import CreateSampleData from "components/pages/database/tasks/createSampleData/CreateSampleData";
import { OngoingTasksPage } from "components/pages/database/tasks/ongoingTasks/OngoingTasksPage";

export = getTasksMenuItem;

function getTasksMenuItem(appUrls: computedAppUrls) {
    const tasksItems: menuItem[] = [
        new leafMenuItem({
            route: 'databases/tasks/backups',
            moduleId: bridgeToReact(BackupsPage, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Backups',
            nav: true,
            css: 'icon-backups',
            dynamicHash: appUrls.backupsUrl,
            search: {
                innerActions: [
                    { name: "Create a backup" },
                    { name: "Create a periodic backup" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/ongoingTasks',
            moduleId: bridgeToReact(OngoingTasksPage, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Ongoing Tasks',
            nav: true,
            css: 'icon-manage-ongoing-tasks',
            dynamicHash: appUrls.ongoingTasksUrl,
            search: {
                innerActions: [
                    {
                        name: "Add a Database Task" ,
                        alternativeNames: [
                            "External Replication",
                            "Replication Hub",
                            "Replication Sink",
                            "RavenDB ETL",
                            "Elastic Search ETL",
                            "Kafka ETL",
                            "SQL ETL",
                            "OLAP ETL",
                            "RabbitMQ ETL",
                            "Kafka Sink",
                            "RabbitMQ Sink",
                            "Periodic Backup",
                            "Subscription",
                        ]
                    },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/import*details',
            moduleId: require('viewmodels/database/tasks/importParent'),
            title: 'Import Data',
            nav: true,
            css: 'icon-import-database',
            dynamicHash: appUrls.importDatabaseFromFileUrl,
            requiredAccess: "DatabaseReadWrite",
            search: {
                innerActions: [
                    {
                        name: "Import database" ,
                        alternativeNames: [
                            "From file",
                            "From RavenDB Server",
                            "From CSV file",
                            "From SQL",
                            "From NoSQL",
                        ]
                    },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/exportDatabase',
            moduleId: require('viewmodels/database/tasks/exportDatabase'),
            shardingMode: "allShards",
            title: 'Export Database',
            nav: true,
            css: 'icon-export-database',
            dynamicHash: appUrls.exportDatabaseUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/sampleData',
            moduleId: bridgeToReact(CreateSampleData, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Create Sample Data',
            nav: true,
            css: 'icon-create-sample-data',
            dynamicHash: appUrls.sampleDataUrl,
            requiredAccess: "DatabaseReadWrite"
        }),
        new leafMenuItem({
            route: 'databases/tasks/editExternalReplicationTask',
            moduleId: require('viewmodels/database/tasks/editExternalReplicationTask'),
            shardingMode: "allShards",
            title: 'External Replication Task',
            nav: false,
            dynamicHash: appUrls.editExternalReplicationTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editReplicationHubTask',
            moduleId: require('viewmodels/database/tasks/editReplicationHubTask'),
            shardingMode: "allShards",
            title: 'Replication Hub Task',
            nav: false,
            dynamicHash: appUrls.editReplicationHubTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editReplicationSinkTask',
            moduleId: require('viewmodels/database/tasks/editReplicationSinkTask'),
            shardingMode: "allShards",
            title: 'Replication Sink Task',
            nav: false,
            dynamicHash: appUrls.editReplicationSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editPeriodicBackupTask',
            moduleId: require('viewmodels/database/tasks/editPeriodicBackupTask'),
            shardingMode: "allShards",
            title: 'Backup Task',
            nav: false,
            dynamicHash: appUrls.backupsUrl,
            itemRouteToHighlight: 'databases/tasks/backups'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSubscriptionTask',
            moduleId: require('viewmodels/database/tasks/editSubscriptionTask'),
            shardingMode: "allShards",
            title: 'Subscription Task',
            nav: false,
            dynamicHash: appUrls.editSubscriptionTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editRavenEtlTask',
            moduleId: require('viewmodels/database/tasks/editRavenEtlTask'),
            shardingMode: "allShards",
            title: 'RavenDB ETL Task',
            nav: false,
            dynamicHash: appUrls.editRavenEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSqlEtlTask',
            moduleId: require('viewmodels/database/tasks/editSqlEtlTask'),
            shardingMode: "allShards",
            title: 'SQL ETL Task',
            nav: false,
            dynamicHash: appUrls.editSqlEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editOlapEtlTask',
            moduleId: require('viewmodels/database/tasks/editOlapEtlTask'),
            shardingMode: "allShards",
            title: 'OLAP ETL Task',
            nav: false,
            dynamicHash: appUrls.editOlapEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editElasticSearchEtlTask',
            moduleId: require('viewmodels/database/tasks/editElasticSearchEtlTask'),
            shardingMode: "allShards",
            title: 'Elastic Search ETL Task',
            nav: false,
            dynamicHash: appUrls.editElasticSearchEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editKafkaEtlTask',
            moduleId: require('viewmodels/database/tasks/editKafkaEtlTask'),
            title: 'Kafka ETL Task',
            nav: false,
            dynamicHash: appUrls.editKafkaEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editRabbitMqEtlTask',
            moduleId: require('viewmodels/database/tasks/editRabbitMqEtlTask'),
            title: 'RabbitMQ ETL Task',
            nav: false,
            dynamicHash: appUrls.editRabbitMqEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editAzureQueueStorageEtlTask',
            moduleId: require('viewmodels/database/tasks/editAzureQueueStorageEtlTask'),
            title: 'Azure Queue Storage ETL Task',
            nav: false,
            dynamicHash: appUrls.editAzureQueueStorageEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editKafkaSinkTask',
            moduleId: require('viewmodels/database/tasks/editKafkaSinkTask'),
            title: 'Kafka Sink Task',
            nav: false,
            dynamicHash: appUrls.editKafkaSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editRabbitMqSinkTask',
            moduleId: require('viewmodels/database/tasks/editRabbitMqSinkTask'),
            title: 'RabbitMQ Sink Task',
            nav: false,
            dynamicHash: appUrls.editRabbitMqSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
    ];

    return new intermediateMenuItem('Tasks', tasksItems, 'icon-tasks-menu');
}
