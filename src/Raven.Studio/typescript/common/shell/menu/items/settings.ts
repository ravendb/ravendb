import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import { bridgeToReact } from "common/reactUtils";
import { ManageDatabaseGroupPage } from "components/pages/resources/manageDatabaseGroup/ManageDatabaseGroupPage";
import ClientDatabaseConfiguration from "components/pages/database/settings/clientConfiguration/ClientDatabaseConfiguration";
import StudioDatabaseConfiguration from "components/pages/database/settings/studioConfiguration/StudioDatabaseConfiguration";
import DocumentRefresh from "components/pages/database/settings/documentRefresh/DocumentRefresh";
import DataArchival from "components/pages/database/settings/dataArchival/DataArchival";
import DocumentExpiration from "components/pages/database/settings/documentExpiration/DocumentExpiration";
import DocumentRevisions from "components/pages/database/settings/documentRevisions/DocumentRevisions";
import TombstonesState from "components/pages/database/settings/tombstones/TombstonesState";
import DatabaseCustomSorters from "components/pages/database/settings/customSorters/DatabaseCustomSorters";
import DatabaseCustomAnalyzers from "components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzers";
import DocumentCompression from "components/pages/database/settings/documentCompression/DocumentCompression";
import RevertRevisions from "components/pages/database/settings/documentRevisions/revertRevisions/RevertRevisions";
import ConnectionStrings from "components/pages/database/settings/connectionStrings/ConnectionStrings";
import DatabaseRecord from "components/pages/database/settings/databaseRecord/DatabaseRecord";
import ConflictResolution from "components/pages/database/settings/conflictResolution/ConflictResolution";
import Integrations from "components/pages/database/settings/integrations/Integrations";
import UnusedDatabaseIds from "components/pages/database/settings/unusedDatabaseIds/UnusedDatabaseIds";

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
            moduleId: bridgeToReact(ConnectionStrings, "nonShardedView"),
            title: "Connection Strings",
            nav: true,
            css: 'icon-manage-connection-strings',
            dynamicHash: appUrls.connectionStrings,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/conflictResolution',
            moduleId: bridgeToReact(ConflictResolution, "nonShardedView"),
            shardingMode: "allShards",
            title: "Conflict Resolution",
            nav: true,
            css: 'icon-conflicts-resolution',
            dynamicHash: appUrls.conflictResolution,
        }),
        new leafMenuItem({
            route: 'databases/settings/clientConfiguration',
            moduleId: bridgeToReact(ClientDatabaseConfiguration, "nonShardedView"),
            search: {
                innerActions: [
                    {
                        name: "Identity parts separator"
                    },
                    {
                        name: "Maximum number of requests per session"
                    }
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
                    {
                        name: "Database Environment",
                    },
                    {
                        name: "Storage Environment",
                    }
                ],
            },
            moduleId: bridgeToReact(StudioDatabaseConfiguration, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Studio Configuration',
            nav: true,
            css: 'icon-database-studio-configuration',
            dynamicHash: appUrls.studioConfiguration,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/revisions',
            moduleId: bridgeToReact(DocumentRevisions, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Document Revisions',
            search: {
                innerActions: [
                    {
                        name: "Revert revisions"
                    },
                    {
                        name: "Enforce revisions configuration"
                    }
                ],
            },
            nav: true,
            css: 'icon-revisions',
            dynamicHash: appUrls.revisions
        }),
        new leafMenuItem({
            route: 'databases/settings/revertRevisions',
            moduleId: bridgeToReact(RevertRevisions, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Revert Revisions',
            nav: false,
            css: 'icon-revert-revisions',
            dynamicHash: appUrls.revertRevisions,
            itemRouteToHighlight: "databases/settings/revisions"
        }),
        new leafMenuItem({
            route: 'databases/settings/refresh',
            moduleId: bridgeToReact(DocumentRefresh, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Document Refresh',
            nav: true,
            css: 'icon-expos-refresh',
            dynamicHash: appUrls.refresh,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/expiration',
            moduleId: bridgeToReact(DocumentExpiration, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Document Expiration',
            nav: true,
            css: 'icon-document-expiration',
            dynamicHash: appUrls.expiration,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/documentsCompression',
            moduleId: bridgeToReact(DocumentCompression, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Document Compression',
            nav: true,
            css: 'icon-documents-compression',
            dynamicHash: appUrls.documentsCompression
        }),
        new leafMenuItem({
            route: 'databases/settings/dataArchival',
            moduleId: bridgeToReact(DataArchival, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Data Archival',
            nav: true,
            css: 'icon-data-archival',
            dynamicHash: appUrls.dataArchival,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/timeSeries',
            moduleId: require('viewmodels/database/settings/timeSeries'),
            shardingMode: "allShards",
            title: 'Time Series',
            nav: true, 
            css: 'icon-timeseries-settings',
            dynamicHash: appUrls.timeSeries
        }),
        new leafMenuItem({
            route: 'databases/settings/customSorters',
            moduleId: bridgeToReact(DatabaseCustomSorters, "nonShardedView"),
            title: 'Custom Sorters',
            shardingMode: "allShards",
            nav: true,
            css: 'icon-custom-sorters',
            dynamicHash: appUrls.customSorters
        }),
        new leafMenuItem({
            route: 'databases/settings/customAnalyzers',
            moduleId: bridgeToReact(DatabaseCustomAnalyzers, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Custom Analyzers',
            nav: true,
            css: 'icon-custom-analyzers',
            dynamicHash: appUrls.customAnalyzers
        }),
        new leafMenuItem({
            route: 'databases/settings/editCustomAnalyzer',
            moduleId: require('viewmodels/database/settings/editCustomAnalyzer'),
            title: 'Custom Analyzer',
            nav: false,
            dynamicHash: appUrls.editCustomAnalyzer,
            itemRouteToHighlight: 'databases/settings/customAnalyzers'
        }),
        new leafMenuItem({
            route: 'databases/manageDatabaseGroup',
            moduleId: bridgeToReact(ManageDatabaseGroupPage, "nonShardedView"),
            title: 'Manage Database Group',
            nav: true,
            css: 'icon-manage-dbgroup',
            dynamicHash: appUrls.manageDatabaseGroup
        }),
        new leafMenuItem({
            route: 'databases/settings/integrations',
            moduleId: bridgeToReact(Integrations, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Integrations',
            nav: true,
            css: 'icon-integrations',
            dynamicHash: appUrls.integrations,
            requiredAccess: "DatabaseAdmin"
        }),
        new separatorMenuItem(),
        new separatorMenuItem('Advanced'),
        new leafMenuItem({
            route: 'databases/advanced/databaseRecord',
            moduleId: bridgeToReact(DatabaseRecord, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Database Record',
            nav: true,
            css: 'icon-database-record',
            dynamicHash: appUrls.databaseRecord,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'databases/advanced/databaseIDs',
            moduleId: bridgeToReact(UnusedDatabaseIds, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Unused Database IDs',
            nav: true,
            css: 'icon-database-id',
            dynamicHash: appUrls.databaseIDs,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'databases/advanced/tombstonesState',
            moduleId: bridgeToReact(TombstonesState, "shardedView"),
            title: 'Tombstones',
            nav: true,
            shardingMode: "singleShard",
            css: 'icon-revisions-bin',
            dynamicHash: appUrls.tombstonesState,
            requiredAccess: "Operator"
        })
    ];

    return new intermediateMenuItem('Settings', settingsItems, 'icon-settings');
}
