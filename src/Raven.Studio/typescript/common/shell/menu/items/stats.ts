import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import { bridgeToReact } from "common/reactUtils";
import { StatisticsPage } from "components/pages/database/status/statistics/StatisticsPage";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import shardedDatabase from "models/resources/shardedDatabase";
import shard from "models/resources/shard";

export = getStatsMenuItem;

function getStatsMenuItem(appUrls: computedAppUrls) {
    const statsItems: menuItem[] = [
        new leafMenuItem({
            route: 'databases/status',
            moduleId: bridgeToReact(StatisticsPage, "nonShardedView"),
            shardingMode: "allShards",
            title: 'Stats',
            nav: true,
            css: 'icon-stats',
            dynamicHash: appUrls.status
        }),
        new leafMenuItem({
            route: 'databases/status/ioStats',
            moduleId: require('viewmodels/database/status/ioStats'),
            shardingMode: "singleShard",
            title: 'IO Stats',
            tooltip: "Displays IO metrics status",
            nav: true,
            css: 'icon-io-test',
            dynamicHash: appUrls.ioStats,
            search: {
                innerActions: [
                    { name: "Export IO Stats" },
                    { name: "Import IO Stats" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/status/storage/report',
            moduleId: require('viewmodels/database/status/storageReport'),
            shardingMode: "singleShard",
            title: 'Storage Report',
            tooltip: "Storage Report",
            nav: true,
            css: 'icon-storage',
            dynamicHash: appUrls.statusStorageReport
        }),
        new leafMenuItem({
            route: 'databases/status/buckets/report',
            moduleId: require('viewmodels/database/status/bucketsReport'),
            shardingMode: "allShards",
            title: 'Buckets Report',
            tooltip: "Buckets Report",
            nav: ko.pureComputed(() => {
                const db = activeDatabaseTracker.default.database();
                if (!db) {
                    return false;
                }
                return (db instanceof shardedDatabase) || (db instanceof shard);
            }),
            css: 'icon-storage', //TODO:
            dynamicHash: appUrls.statusBucketsReport
        }),
        new leafMenuItem({
            route: "virtual", // here we only redirect to global section with proper db set in url
            moduleId: () => { /* empty */},
            title: 'Running Queries',
            nav: true,
            css: 'icon-stats-running-queries',
            dynamicHash: appUrls.runningQueries
        }),
        new leafMenuItem({
            route: 'databases/status/ongoingTasksStats',
            moduleId: require('viewmodels/database/status/ongoingTasksStats'),
            shardingMode: "singleShard",
            title: 'Ongoing Tasks Stats',
            nav: true,
            css: 'icon-replication-stats',
            dynamicHash: appUrls.ongoingTasksStats,
            search: {
                innerActions: [
                    { name: "Export Ongoing Tasks Stats" },
                    { name: "Import Ongoing Tasks Stats" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/status/debug*details',
            moduleId: null,
            title: 'Debug',
            nav: false,
            css: 'icon-debug',
            search: {
                isExcluded: true
            }
        })
    ];

    return new intermediateMenuItem("Stats", statsItems, "icon-stats-menu");
}
