import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getStatsMenuItem;

function getStatsMenuItem(appUrls: computedAppUrls) {
    let activeDatabase = activeDatabaseTracker.default.database;
    var items: menuItem[] = [
        new leafMenuItem({
            route: 'databases/status',
            moduleId: 'viewmodels/database/status/statistics',
            title: 'Stats',
            nav: true,
            css: 'icon-stats',
            dynamicHash: appUrls.status
        }),
        new leafMenuItem({
            route: 'databases/status/ioStats',
            moduleId: 'viewmodels/database/status/ioStats',
            title: 'IO Stats',
            tooltip: "Displays IO metrics statatus",
            nav: true,
            css: 'icon-io-test',
            dynamicHash: appUrls.ioStats
        }),
        new leafMenuItem({
            route: 'databases/status/storage/report',
            moduleId: 'viewmodels/database/status/storageReport',
            title: 'Storage Report',
            tooltip: "Storage Report",
            nav: true,
            css: 'icon-storage',
            dynamicHash: appUrls.statusStorageReport
        }),
        new leafMenuItem({
            route: 'databases/status/ongoingTasksStats',
            moduleId: 'viewmodels/database/status/ongoingTasksStats',
            title: 'Ongoing Tasks Stats',
            nav: true,
            css: 'icon-replication-stats', //TODO:
            dynamicHash: appUrls.ongoingTasksStats
        }),
        new leafMenuItem({
            route: 'databases/status/debug*details',
            moduleId: 'viewmodels/database/status/debug/statusDebug',
            title: 'Debug',
            nav: false,
            css: 'icon-debug'
        })
       
    ];

    return new intermediateMenuItem("Stats", items, "icon-stats");
}
