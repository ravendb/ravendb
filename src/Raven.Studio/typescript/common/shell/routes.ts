
import MENU_BASED_ROUTER_CONFIGURATION = require("common/shell/routerConfiguration");

export = Routes;

class Routes {

    static get(appUrls: computedAppUrls): Array<DurandalRouteConfiguration> {
        let routes = [
            {
                route: "databases/upgrade",
                title: "Upgrade in progress",
                moduleId: "viewmodels/common/upgrade",
                nav: false,
                dynamicHash: appUrls.upgrade
            },
            {
                route: "databases/edit",
                title: "Edit Document",
                moduleId: "viewmodels/database/documents/editDocument",
                nav: false
            },
            {
                route: "filesystems/files",
                title: "Files",
                moduleId: "viewmodels/filesystem/files/filesystemFiles",
                nav: true,
                dynamicHash: appUrls.filesystemFiles
            },
            {
                route: "filesystems/search",
                title: "Search",
                moduleId: "viewmodels/filesystem/search/search",
                nav: true,
                dynamicHash: appUrls.filesystemSearch
            },
            {
                route: "filesystems/synchronization*details",
                title: "Synchronization",
                moduleId: "viewmodels/filesystem/synchronization/synchronization",
                nav: true,
                dynamicHash: appUrls.filesystemSynchronization
            },
            {
                route: "filesystems/status*details",
                title: "Status",
                moduleId: "viewmodels/filesystem/status/status",
                nav: true,
                dynamicHash: appUrls.filesystemStatus
            },
            {
                route: "filesystems/tasks*details",
                title: "Tasks",
                moduleId: "viewmodels/filesystem/tasks/tasks",
                nav: true,
                dynamicHash: appUrls.filesystemTasks
            },
            {
                route: "filesystems/settings*details",
                title: "Settings",
                moduleId: "viewmodels/filesystem/settings/settings",
                nav: true,
                dynamicHash: appUrls.filesystemSettings
            },
            {
                route: "filesystems/configuration",
                title: "Configuration",
                moduleId: "viewmodels/filesystem/configurations/configuration",
                nav: true,
                dynamicHash: appUrls.filesystemConfiguration
            },
            {
                route: "filesystems/edit",
                title: "Edit File",
                moduleId: "viewmodels/filesystem/files/filesystemEditFile",
                nav: false
            },
            {
                route: "counterstorages/counters",
                title: "Counters",
                moduleId: "viewmodels/counter/counters",
                nav: true,
                dynamicHash: appUrls.counterStorageCounters
            },
            {
                route: "counterstorages/replication",
                title: "Replication",
                moduleId: "viewmodels/counter/counterStorageReplication",
                nav: true,
                dynamicHash: appUrls.counterStorageReplication
            },
            {
                route: "counterstorages/tasks*details",
                title: "Stats",
                moduleId: "viewmodels/counter/tasks/tasks",
                nav: true,
                dynamicHash: appUrls.counterStorageStats
            },
            {
                route: "counterstorages/stats",
                title: "Stats",
                moduleId: "viewmodels/counter/counterStorageStats",
                nav: true,
                dynamicHash: appUrls.counterStorageStats
            },
            {
                route: "counterstorages/configuration",
                title: "Configuration",
                moduleId: "viewmodels/counter/counterStorageConfiguration",
                nav: true,
                dynamicHash: appUrls.counterStorageConfiguration
            },
            {
                route: "counterstorages/edit",
                title: "Edit Counter",
                moduleId: "viewmodels/counter/editCounter",
                nav: false
            },
            {
                route: "timeseries/types",
                title: "Types",
                moduleId: "viewmodels/timeSeries/timeSeriesTypes",
                nav: true,
                dynamicHash: appUrls.timeSeriesType
            },
            {
                route: "timeseries/points",
                title: "Points",
                moduleId: "viewmodels/timeSeries/timeSeriesPoints",
                nav: true,
                dynamicHash: appUrls.timeSeriesPoints
            },
            {
                route: "timeseries/stats",
                title: "Stats",
                moduleId: "viewmodels/timeSeries/timeSeriesStats",
                nav: true,
                dynamicHash: appUrls.timeSeriesStats
            },
            {
                route: "timeseries/configuration*details",
                title: "Configuration",
                moduleId: "viewmodels/timeSeries/configuration/configuration",
                nav: true,
                dynamicHash: appUrls.timeSeriesConfiguration
            }
        ] as Array<DurandalRouteConfiguration>;

        return routes.concat(MENU_BASED_ROUTER_CONFIGURATION);
    }

}

