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
        }),
        new leafMenuItem({
            route: 'databases/tasks/editPeriodicBackupTask',
            moduleId: require('viewmodels/database/tasks/editPeriodicBackupTask'),
            shardingMode: "allShards",
            title: 'Backup Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editPeriodicBackupTask("Backups", true),
            itemRouteToHighlight: 'databases/tasks/backups',
            search: {
                overrideTitle: "Add New Backup Task",
                alternativeTitles: ["Create Manual Backup Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editPeriodicBackupTask',
            moduleId: require('viewmodels/database/tasks/editPeriodicBackupTask'),
            shardingMode: "allShards",
            title: 'Backup Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editPeriodicBackupTask("Backups", false),
            itemRouteToHighlight: 'databases/tasks/backups',
            search: {
                overrideTitle: "Add New Periodic Backup Task",
                alternativeTitles: ["Create Periodic Backup Task"],
            }
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
                alternativeTitles:[
                    "ETL",
                    "Hub",
                    "Sink",
                    "Subscription",
                    "Replication",
                ],
                innerActions: [
                    { name: "Enable Database Task" },
                    { name: "Disable Database Task" },
                    { name: "Edit Database Task" },
                    { name: "Delete Database Task", alternativeNames: ["Remove Database Task"] },
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
        }),
        new leafMenuItem({
            route: 'databases/tasks/import/file',
            moduleId: require('viewmodels/database/tasks/importDatabaseFromFile'),
            title: 'Import Data',
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.importDatabaseFromFileUrl,
            itemRouteToHighlight: 'databases/tasks/import*details',
            search: {
                overrideTitle: "Import Database From File",
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/import/migrateRavenDB',
            moduleId: require('viewmodels/database/tasks/migrateRavenDbDatabase'),
            title: 'Import Data',
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.migrateRavenDbDatabaseUrl,
            itemRouteToHighlight: 'databases/tasks/import*details',
            search: {
                overrideTitle: "Import Database From RavenDB Server",
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/import/csv',
            moduleId: require('viewmodels/database/tasks/importCollectionFromCsv'),
            title: 'Import Data',
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.importCollectionFromCsv,
            itemRouteToHighlight: 'databases/tasks/import*details',
            search: {
                overrideTitle: "Import documents from a CSV",
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/import/sql',
            moduleId: require('viewmodels/database/tasks/importDatabaseFromSql'),
            title: 'Import Data',
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.importDatabaseFromSql,
            itemRouteToHighlight: 'databases/tasks/import*details',
            search: {
                overrideTitle: "Import documents from a SQL",
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/migrate',
            moduleId: require('viewmodels/database/tasks/migrateDatabase'),
            title: 'Import Data',
            nav: false,
            css: 'icon-export-database',
            dynamicHash: appUrls.exportDatabaseUrl,
            itemRouteToHighlight: 'databases/tasks/export*details',
            search: {
                overrideTitle: "Migrate data from another database",
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
            css: "icon-plus",
            dynamicHash: appUrls.editExternalReplicationTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New External Replication Task",
                alternativeTitles: ["Create External Replication Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editReplicationHubTask',
            moduleId: require('viewmodels/database/tasks/editReplicationHubTask'),
            shardingMode: "allShards",
            title: 'Replication Hub Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editReplicationHubTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New Replication Hub Task",
                alternativeTitles: ["Create Replication Hub Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editReplicationSinkTask',
            moduleId: require('viewmodels/database/tasks/editReplicationSinkTask'),
            shardingMode: "allShards",
            title: 'Replication Sink Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editReplicationSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New Replication Sink Task",
                alternativeTitles: ["Create Replication Sink Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSubscriptionTask',
            moduleId: require('viewmodels/database/tasks/editSubscriptionTask'),
            shardingMode: "allShards",
            title: 'Subscription Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editSubscriptionTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New Subscription Task",
                alternativeTitles: ["Create Subscription Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editRavenEtlTask',
            moduleId: require('viewmodels/database/tasks/editRavenEtlTask'),
            shardingMode: "allShards",
            title: 'RavenDB ETL Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editRavenEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New RavenDB ETL Task",
                alternativeTitles: ["Create RavenDB ETL Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSqlEtlTask',
            moduleId: require('viewmodels/database/tasks/editSqlEtlTask'),
            shardingMode: "allShards",
            title: 'SQL ETL Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editSqlEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New SQL ETL Task",
                alternativeTitles: ["Create SQL ETL Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSnowflakeEtlTask',
            moduleId: require('viewmodels/database/tasks/editSnowflakeEtlTask'),
            shardingMode: "allShards",
            title: 'Snowflake ETL Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editSnowflakeEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New Snowflake ETL Task",
                alternativeTitles: ["Create Snowflake ETL Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editOlapEtlTask',
            moduleId: require('viewmodels/database/tasks/editOlapEtlTask'),
            shardingMode: "allShards",
            title: 'OLAP ETL Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editOlapEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New OLAP ETL Task",
                alternativeTitles: ["Create OLAP ETL Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editElasticSearchEtlTask',
            moduleId: require('viewmodels/database/tasks/editElasticSearchEtlTask'),
            shardingMode: "allShards",
            title: 'Elastic Search ETL Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editElasticSearchEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New Elastic Search ETL Task",
                alternativeTitles: ["Create Elastic Search ETL Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editKafkaEtlTask',
            moduleId: require('viewmodels/database/tasks/editKafkaEtlTask'),
            title: 'Kafka ETL Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editKafkaEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New Kafka ETL Task",
                alternativeTitles: ["Create Kafka ETL Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editRabbitMqEtlTask',
            moduleId: require('viewmodels/database/tasks/editRabbitMqEtlTask'),
            title: 'RabbitMQ ETL Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editRabbitMqEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New RabbitMQ ETL Task",
                alternativeTitles: ["Create RabbitMQ ETL Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editAzureQueueStorageEtlTask',
            moduleId: require('viewmodels/database/tasks/editAzureQueueStorageEtlTask'),
            title: 'Azure Queue Storage ETL Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editAzureQueueStorageEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New Azure Queue Storage ETL Task",
                alternativeTitles: ["Create Azure Queue Storage ETL Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editKafkaSinkTask',
            moduleId: require('viewmodels/database/tasks/editKafkaSinkTask'),
            title: 'Kafka Sink Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editKafkaSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New Kafka Sink Task",
                alternativeTitles: ["Create Kafka Sink Task"],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editRabbitMqSinkTask',
            moduleId: require('viewmodels/database/tasks/editRabbitMqSinkTask'),
            title: 'RabbitMQ Sink Task',
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editRabbitMqSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: "Add New RabbitMQ Sink Task",
                alternativeTitles: ["Create RabbitMQ Sink Task"],
            }
        }),
    ];

    return new intermediateMenuItem('Tasks', tasksItems, 'icon-tasks-menu');
}
