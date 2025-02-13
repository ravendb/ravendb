import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import reactUtils = require("common/reactUtils");
import ManageDatabaseGroupPage = require("components/pages/resources/manageDatabaseGroup/ManageDatabaseGroupPage");
import ClientDatabaseConfiguration = require("components/pages/database/settings/clientConfiguration/ClientDatabaseConfiguration");
import StudioDatabaseConfiguration = require("components/pages/database/settings/studioConfiguration/StudioDatabaseConfiguration");
import DocumentRefresh = require("components/pages/database/settings/documentRefresh/DocumentRefresh");
import DataArchival = require("components/pages/database/settings/dataArchival/DataArchival");
import DocumentExpiration = require("components/pages/database/settings/documentExpiration/DocumentExpiration");
import DocumentRevisions = require("components/pages/database/settings/documentRevisions/DocumentRevisions");
import TombstonesState = require("components/pages/database/settings/tombstones/TombstonesState");
import DatabaseCustomSorters = require("components/pages/database/settings/customSorters/DatabaseCustomSorters");
import DatabaseCustomAnalyzers = require("components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzers");
import DocumentCompression = require("components/pages/database/settings/documentCompression/DocumentCompression");
import RevertRevisions = require("components/pages/database/settings/documentRevisions/revertRevisions/RevertRevisions");
import ConnectionStrings = require("components/pages/database/settings/connectionStrings/ConnectionStrings");
import DatabaseRecord = require("components/pages/database/settings/databaseRecord/DatabaseRecord");
import ConflictResolution = require("components/pages/database/settings/conflictResolution/ConflictResolution");
import Integrations = require("components/pages/database/settings/integrations/Integrations");
import UnusedDatabaseIds = require("components/pages/database/settings/unusedDatabaseIds/UnusedDatabaseIds");
import RevisionsBinCleaner = require("components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleaner");

export = getSettingsMenuItem;

function getSettingsMenuItem(appUrls: computedAppUrls) {
    
    const settingsItems: menuItem[] = [
        new leafMenuItem({
            route: 'databases/settings/databaseSettings',
            moduleId: require('viewmodels/database/settings/databaseSettings'),
            shardingMode: "allShards",
            title: 'Database Settings',
            nav: true,
            css: 'icon-database-settings',
            dynamicHash: appUrls.databaseSettings,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'databases/settings/connectionStrings',
            moduleId: reactUtils.bridgeToReact(ConnectionStrings.default, "nonShardedView"),
            title: "Connection Strings",
            nav: true,
            css: 'icon-manage-connection-strings',
            dynamicHash: appUrls.connectionStrings,
            requiredAccess: "DatabaseAdmin",
            search: {
                innerActions: [
                    {
                        name: "Add New Connection String",
                        alternativeNames: [
                            "Create Connection String",
                            "RavenDB",
                            "SQL",
                            "OLAP",
                            "ElasticSearch",
                            "Kafka",
                            "RabbitMQ",
                        ],
                    },
                    {
                        name: "Delete connection string",
                        alternativeNames: ["Remove connection string"]
                    },
                    { name: "Edit connection string" },
                    { name: "Test connection string" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/conflictResolution',
            moduleId: reactUtils.bridgeToReact(ConflictResolution.default, "nonShardedView"),
            shardingMode: "allShards",
            title: "Conflict Resolution",
            nav: true,
            css: 'icon-conflicts-resolution',
            dynamicHash: appUrls.conflictResolution,
        }),
        new leafMenuItem({
            route: 'databases/settings/clientConfiguration',
            moduleId: reactUtils.bridgeToReact(ClientDatabaseConfiguration.default, "nonShardedView"),
            search: {
                innerActions: [
                    { name: "Identity Parts Separator" },
                    { name: "Maximum Number of Requests per Session" },
                    { name: "Load Balance Behavior" },
                    { name: "Seed" },
                    { name: "Read Balance Behavior" },
                ],
            },
            shardingMode: "allShards",
            title: 'Client Configuration',
            nav: true,
            css: 'icon-database-client-configuration',
            dynamicHash: appUrls.clientConfiguration,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/studioConfiguration',
            search: {
                innerActions: [
                    { name: "Database Environment" },
                    { name: "Disable Creating New Auto-Indexes" }
                ],
            },
            moduleId: reactUtils.bridgeToReact(StudioDatabaseConfiguration.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Studio Configuration',
            nav: true,
            css: 'icon-database-studio-configuration',
            dynamicHash: appUrls.studioConfiguration,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/refresh',
            moduleId: reactUtils.bridgeToReact(DocumentRefresh.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Document Refresh',
            nav: true,
            css: 'icon-expos-refresh',
            dynamicHash: appUrls.refresh,
            requiredAccess: "DatabaseAdmin",
            search: {
                innerActions: [
                    { name: "Enable Document Refresh" },
                    { name: "Set custom refresh frequency" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/expiration',
            moduleId: reactUtils.bridgeToReact(DocumentExpiration.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Document Expiration',
            nav: true,
            css: 'icon-document-expiration',
            dynamicHash: appUrls.expiration,
            requiredAccess: "DatabaseAdmin",
            search: {
                innerActions: [
                    { name: "Enable Document Expiration" },
                    { name: "Set custom expiration frequency" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/documentsCompression',
            moduleId: reactUtils.bridgeToReact(DocumentCompression.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Document Compression',
            nav: true,
            css: 'icon-documents-compression',
            dynamicHash: appUrls.documentsCompression,
            search: {
                innerActions: [
                    { name: "Compress revisions for all collections" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/revisions',
            moduleId: reactUtils.bridgeToReact(DocumentRevisions.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Document Revisions',
            search: {
                innerActions: [
                    { name: "Enforce Revisions Configuration" },
                    { name: "Add New Revision Configuration", alternativeNames: ["Create Revision Configuration"] },
                    { name: "Delete Revision Configuration", alternativeNames: ["Remove Revision Configuration"] },
                    { name: "Edit Revision Configuration" },
                    { name: "Enable Revision Configuration" },
                    { name: "Disable Revision Configuration" },
                ],
            },
            nav: true,
            css: 'icon-revisions',
            dynamicHash: appUrls.revisions
        }),
        new leafMenuItem({
            route: 'databases/settings/revisionsBinCleaner',
            moduleId: reactUtils.bridgeToReact(RevisionsBinCleaner.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Revisions Bin Cleaner',
            nav: true,
            css: 'icon-revisions-bin',
            dynamicHash: appUrls.revisionsBinCleaner,
            search: {
                innerActions: [
                    { name: "Set automatic revision bin configuration" },
                    { name: "Configure retention policies" },
                    { name: "View current revisions bin configuration" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/revertRevisions',
            moduleId: reactUtils.bridgeToReact(RevertRevisions.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Revert Revisions',
            nav: false,
            css: 'icon-revert-revisions',
            dynamicHash: appUrls.revertRevisions,
            itemRouteToHighlight: "databases/settings/revisions",
        }),
        new leafMenuItem({
            route: 'databases/settings/dataArchival',
            moduleId: reactUtils.bridgeToReact(DataArchival.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Data Archival',
            nav: true,
            css: 'icon-data-archival',
            dynamicHash: appUrls.dataArchival,
            requiredAccess: "DatabaseAdmin",
            search: {
                innerActions: [
                    { name: "Enable Data Archival" },
                    { name: "Set custom archival frequency" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/timeSeries',
            moduleId: require('viewmodels/database/settings/timeSeries'),
            shardingMode: "allShards",
            title: 'Time Series',
            nav: true, 
            css: 'icon-timeseries-settings',
            dynamicHash: appUrls.timeSeries,
            search: {
                innerActions: [
                    { name: "Enable Time Series" },
                    { name: "Disable Time Series" },
                    { name: "Policy Check Frequency" },
                    { name: "Delete Time Series", alternativeNames: ["Remove Time Series"] },
                    { name: "Edit Time Series" },
                    { name: "Add New Collection Specific Configuration", alternativeNames: ["Create Collection Specific Configuration"] },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/customSorters',
            moduleId: reactUtils.bridgeToReact(DatabaseCustomSorters.default, "nonShardedView"),
            title: 'Custom Sorters',
            shardingMode: "allShards",
            nav: true,
            css: 'icon-custom-sorters',
            dynamicHash: appUrls.customSorters,
            search: {
                innerActions: [
                    { name: "Add New Custom Sorter", alternativeNames: ["Create Custom Sorter"] },
                    { name: "Delete custom sorter", alternativeNames: ["Remove Custom Sorter"] },
                    { name: "Edit custom sorter" },
                    { name: "Test custom sorter" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/customAnalyzers',
            moduleId: reactUtils.bridgeToReact(DatabaseCustomAnalyzers.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Custom Analyzers',
            nav: true,
            css: 'icon-custom-analyzers',
            dynamicHash: appUrls.customAnalyzers,
            search: {
                innerActions: [
                    { name: "Add New Custom Analyzer", alternativeNames: ["Create Custom Analyzer"] },
                    { name: "Delete custom analyzer", alternativeNames: ["Remove Custom Analyzer"] },
                    { name: "Edit custom analyzer" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/manageDatabaseGroup',
            moduleId: reactUtils.bridgeToReact(ManageDatabaseGroupPage.ManageDatabaseGroupPage, "nonShardedView"),
            title: 'Manage Database Group',
            nav: true,
            css: 'icon-manage-dbgroup',
            dynamicHash: appUrls.manageDatabaseGroup,
            search: {
                innerActions: [
                    { name: "Add node" },
                    { name: "Reorder nodes" },
                    { name: "Allow dynamic database distribution" },
                    { name: "Add shard" },
                    { name: "Delete from group", alternativeNames: ["Remove from group"] },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/integrations',
            moduleId: reactUtils.bridgeToReact(Integrations.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Integrations',
            nav: true,
            css: 'icon-integrations',
            dynamicHash: appUrls.integrations,
            requiredAccess: "DatabaseAdmin",
            search: {
                alternativeTitles: ["PostgreSQL protocol credentials"],
                innerActions: [
                    { name: "Add New Credentials", alternativeNames: ["Create Credentials"] },
                    { name: "Delete credentials", alternativeNames: ["Remove Credentials"] },
                ]
            }
        }),
        new separatorMenuItem(),
        new separatorMenuItem('Advanced'),
        new leafMenuItem({
            route: 'databases/advanced/databaseRecord',
            moduleId: reactUtils.bridgeToReact(DatabaseRecord.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Database Record',
            nav: true,
            css: 'icon-database-record',
            dynamicHash: appUrls.databaseRecord,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'databases/advanced/databaseIDs',
            moduleId: reactUtils.bridgeToReact(UnusedDatabaseIds.default, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Unused Database IDs',
            nav: true,
            css: 'icon-database-id',
            dynamicHash: appUrls.databaseIDs,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'databases/advanced/tombstonesState',
            moduleId: reactUtils.bridgeToReact(TombstonesState.default, "shardedView"),
            title: 'Tombstones',
            nav: true,
            shardingMode: "singleShard",
            css: 'icon-tombstones',
            dynamicHash: appUrls.tombstonesState,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: "Force cleanup" },
                ],
            },
        })
    ];

    return new intermediateMenuItem('Settings', settingsItems, 'icon-settings');
}
