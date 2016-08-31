import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getSettingsMenuItem;

function getSettingsMenuItem(appUrls: computedAppUrls) {
    var items: menuItem[] = [
        new leafMenuItem({
            route: ['databases/settings', 'databases/settings/databaseSettings'],
            moduleId: 'viewmodels/database/settings/databaseSettings',
            title: 'Database Settings',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.databaseSettings
        }),
        new leafMenuItem({
            route: 'databases/settings/quotas',
            moduleId: 'viewmodels/database/settings/quotas',
            title: 'Quotas',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.quotas
        }),
        new leafMenuItem({
            route: 'databases/settings/replication',
            moduleId: 'viewmodels/database/settings/replications',
            title: 'Replication',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.replications
        }),
        new leafMenuItem({
            route: 'databases/settings/etl',
            moduleId: 'viewmodels/database/settings/etl',
            title: 'ETL',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.etl
        }),
        new leafMenuItem({
            route: 'databases/settings/sqlReplication',
            moduleId: 'viewmodels/database/settings/sqlReplications',
            title: 'SQL Replication',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.sqlReplications
        }),
        new leafMenuItem({
            route: 'databases/settings/editSqlReplication(/:sqlReplicationName)',
            moduleId: 'viewmodels/database/settings/editSqlReplication',
            title: 'Edit SQL Replication',
            nav: false,
            css: 'icon-plus',
            dynamicHash: appUrls.editSqlReplication
        }),
        new leafMenuItem({
            route: 'databases/settings/sqlReplicationConnectionStringsManagement',
            moduleId: 'viewmodels/database/settings/sqlReplicationConnectionStringsManagement',
            title: 'SQL Replication Connection Strings',
            nav: false,
            css: 'icon-plus',
            dynamicHash: appUrls.sqlReplicationsConnections
        }),
        new leafMenuItem({
            route: 'databases/settings/versioning',
            moduleId: 'viewmodels/database/settings/versioning',
            title: 'Versioning',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.versioning
        }),
        new leafMenuItem({
            route: 'databases/settings/periodicExport',
            moduleId: 'viewmodels/database/settings/periodicExport',
            title: 'Periodic Export',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.periodicExport
        }),
        new leafMenuItem({
            route: 'databases/settings/customFunctionsEditor',
            moduleId: 'viewmodels/database/settings/customFunctionsEditor',
            title: 'Custom Functions',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.customFunctionsEditor
        }),
        new leafMenuItem({
            route: 'databases/settings/databaseStudioConfig',
            moduleId: 'viewmodels/databaseStudioConfig',
            title: 'Studio Config',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.databaseStudioConfig
        })
    ];

    return new intermediateMenuItem('Settings', items, 'icon-settings');
}
