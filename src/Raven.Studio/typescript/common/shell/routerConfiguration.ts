import appUrl = require("common/appUrl");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

import getManageServerMenuItem = require("common/shell/menu/items/manageServer");
import getDatabasesMenuItem = require("common/shell/menu/items/databases");
import getSettingsMenuItem = require("common/shell/menu/items/settings");
import getStatsMenuItem = require("common/shell/menu/items/stats");
import getTasksMenuItem = require("common/shell/menu/items/tasks");
import getIndexesMenuItem = require("common/shell/menu/items/indexes");
import getDocumentsMenuItem = require("common/shell/menu/items/documents");
import rootItems = require("common/shell/menu/items/rootItems");

export = getRouterConfiguration();

function getRouterConfiguration(): Array<DurandalRouteConfiguration> {
    return generateAllMenuItems()
        .map(getMenuItemDurandalRoutes)
        .reduce((result, next) => result.concat(next), [])
        .reduce((result: any[], next: any) => {
            const nextJson = JSON.stringify(next);
            if (!result.some(x => JSON.stringify(x) === nextJson)) {
                result.push(next);
            }

            return result;
        }, []) as Array<DurandalRouteConfiguration>;
}

function convertToDurandalRoute(leaf: leafMenuItem): DurandalRouteConfiguration {
    return {
        route: leaf.route,
        title: leaf.title,
        moduleId: leaf.moduleId,
        nav: leaf.nav,
        dynamicHash: leaf.dynamicHash,
        requiredAccess: leaf.requiredAccess
    };
}

function getMenuItemDurandalRoutes(item: menuItem): Array<DurandalRouteConfiguration> {
    if (item.type === 'intermediate') {
        const intermediateItem = item as intermediateMenuItem;
        return intermediateItem.children
            .map(child => getMenuItemDurandalRoutes(child))
            .reduce((result, next) => result.concat(next), []);
    } else if (item.type === 'leaf') {
        return [convertToDurandalRoute(item as leafMenuItem)];
    }

    return [];
}

function generateAllMenuItems() {
    let appUrls = appUrl.forCurrentDatabase();
    return [
        getDocumentsMenuItem(appUrls),
        getIndexesMenuItem(appUrls),
        getSettingsMenuItem(appUrls),
        getTasksMenuItem(appUrls),
        getStatsMenuItem(appUrls),
        getDatabasesMenuItem(appUrls),
        getManageServerMenuItem(),
        rootItems.about(),
        rootItems.clusterDashboard(),
        rootItems.dashboard()
    ];
}



