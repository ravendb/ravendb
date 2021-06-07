import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");

export = getSettingsMenuItem;

function getSettingsMenuItem(appUrls: computedAppUrls) {
    
    const settingsItems: menuItem[] = [
        new leafMenuItem({
            route: ['databases/settings/databaseSettings'],
            moduleId: 'viewmodels/database/settings/databaseSettings',
            title: 'Database Settings',
            nav: true,
            css: 'icon-database-settings',
            dynamicHash: appUrls.databaseSettings,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'databases/settings/connectionStrings',
            moduleId: "viewmodels/database/settings/connectionStrings",
            title: "Connection Strings",
            nav: true,
            css: 'icon-manage-connection-strings',
            dynamicHash: appUrls.connectionStrings,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/conflictResolution',
            moduleId: "viewmodels/database/settings/conflictResolution",
            title: "Conflict Resolution",
            nav: true,
            css: 'icon-conflicts-resolution',
            dynamicHash: appUrls.conflictResolution,
        }),
        new leafMenuItem({
            route: 'databases/settings/clientConfiguration',
            moduleId: 'viewmodels/database/settings/clientConfiguration',
            title: 'Client Configuration',
            nav: true,
            css: 'icon-database-client-configuration',
            dynamicHash: appUrls.clientConfiguration
        }),
        new leafMenuItem({
            route: 'databases/settings/studioConfiguration',
            moduleId: 'viewmodels/database/settings/studioConfiguration',
            title: 'Studio Configuration',
            nav: true,
            css: 'icon-database-studio-configuration',
            dynamicHash: appUrls.studioConfiguration
        }),
        new leafMenuItem({
            route: 'databases/settings/revisions',
            moduleId: 'viewmodels/database/settings/revisions',
            title: 'Document Revisions',
            nav: true,
            css: 'icon-revisions',
            dynamicHash: appUrls.revisions
        }),
        new leafMenuItem({
            route: 'databases/settings/revertRevisions',
            moduleId: 'viewmodels/database/settings/revertRevisions',
            title: 'Revert Revisions',
            nav: false,
            css: 'icon-revert-revisions',
            dynamicHash: appUrls.revertRevisions,
            itemRouteToHighlight: "databases/settings/revisions"
        }),
        new leafMenuItem({
            route: 'databases/settings/refresh',
            moduleId: 'viewmodels/database/settings/refresh',
            title: 'Document Refresh',
            nav: true,
            css: 'icon-expos-refresh',
            dynamicHash: appUrls.refresh
        }),
        new leafMenuItem({
            route: 'databases/settings/expiration',
            moduleId: 'viewmodels/database/settings/expiration',
            title: 'Document Expiration',
            nav: true,
            css: 'icon-document-expiration',
            dynamicHash: appUrls.expiration
        }),
        new leafMenuItem({
            route: 'databases/settings/documentsCompression',
            moduleId: 'viewmodels/database/settings/documentsCompression',
            title: 'Document Compression',
            nav: true,
            css: 'icon-documents-compression',
            dynamicHash: appUrls.documentsCompression
        }),
        new leafMenuItem({
            route: 'databases/settings/timeSeries',
            moduleId: 'viewmodels/database/settings/timeSeries',
            title: 'Time Series',
            nav: true, 
            css: 'icon-timeseries-settings',
            dynamicHash: appUrls.timeSeries
        }),
        new leafMenuItem({
            route: 'databases/settings/customSorters',
            moduleId: 'viewmodels/database/settings/customSorters',
            title: 'Custom Sorters',
            nav: true,
            css: 'icon-custom-sorters',
            dynamicHash: appUrls.customSorters
        }),
        new leafMenuItem({
            route: 'databases/settings/customAnalyzers',
            moduleId: 'viewmodels/database/settings/customAnalyzers',
            title: 'Custom Analyzers',
            nav: true,
            css: 'icon-custom-analyzers',
            dynamicHash: appUrls.customAnalyzers
        }),
        new leafMenuItem({
            route: 'databases/settings/editCustomSorter',
            moduleId: 'viewmodels/database/settings/editCustomSorter',
            title: 'Custom Sorter',
            nav: false,
            dynamicHash: appUrls.editCustomSorter, 
            itemRouteToHighlight: 'databases/settings/customSorters'
        }),
        new leafMenuItem({
            route: 'databases/settings/editCustomAnalyzer',
            moduleId: 'viewmodels/database/settings/editCustomAnalyzer',
            title: 'Custom Analyzer',
            nav: false,
            dynamicHash: appUrls.editCustomAnalyzer,
            itemRouteToHighlight: 'databases/settings/customAnalyzers'
        }),
        new leafMenuItem({
            route: 'databases/manageDatabaseGroup',
            moduleId: 'viewmodels/resources/manageDatabaseGroup',
            title: 'Manage Database Group',
            nav: true,
            css: 'icon-manage-dbgroup',
            dynamicHash: appUrls.manageDatabaseGroup
        }),
        new separatorMenuItem(),
        new separatorMenuItem('Advanced'),
        new leafMenuItem({
            route: 'databases/advanced/databaseRecord',
            moduleId: 'viewmodels/database/advanced/databaseRecord',
            title: 'Database Record',
            nav: true,
            css: 'icon-database-record',
            dynamicHash: appUrls.databaseRecord,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'databases/advanced/databaseIDs',
            moduleId: 'viewmodels/database/advanced/databaseIDs',
            title: 'Unused Database IDs',
            nav: true,
            css: 'icon-database-id',
            dynamicHash: appUrls.databaseIDs,
            requiredAccess: "Operator"
        })
    ];

    return new intermediateMenuItem('Settings', settingsItems, 'icon-settings');
}
