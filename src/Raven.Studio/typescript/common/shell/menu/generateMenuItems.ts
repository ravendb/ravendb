/// <reference path="../../../../typings/tsd.d.ts" />

import appUrl = require("common/appUrl");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import database = require("models/resources/database");

import getManageServerMenuItem = require("common/shell/menu/items/manageServer");
import getDatabasesMenuItem = require("common/shell/menu/items/databases");
import getSettingsMenuItem = require("common/shell/menu/items/settings");
import getStatsMenuItem = require("common/shell/menu/items/stats");
import getTasksMenuItem = require("common/shell/menu/items/tasks");
import getIndexesMenuItem = require("common/shell/menu/items/indexes");
import getDocumentsMenuItem = require("common/shell/menu/items/documents");
import rootItems = require("common/shell/menu/items/rootItems");

export = generateMenuItems;

interface GenerateMenuItemsOptions {
    db: database | string;
    isNewVersionAvailable: boolean;
    isWhatsNewVisible: boolean;
}

function generateMenuItems(options: GenerateMenuItemsOptions) {
    if (!options.db) {
        return generateNoActiveDatabaseMenuItems(options);
    } 

    return generateActiveDatabaseMenuItems(options);
}

function generateNoActiveDatabaseMenuItems(options: GenerateMenuItemsOptions) {
    const appUrls = appUrl.forCurrentDatabase();
    return [
        new separatorMenuItem('Server'),
        getDatabasesMenuItem(appUrls),
        rootItems.clusterDashboard(),
        getManageServerMenuItem(),
        rootItems.about(),
        rootItems.whatsNew(options),
    ];
}

function generateActiveDatabaseMenuItems(options: GenerateMenuItemsOptions) {
    const appUrls = appUrl.forCurrentDatabase();
    return [
        getDocumentsMenuItem(appUrls),
        getIndexesMenuItem(appUrls),        
        getTasksMenuItem(appUrls),
        getSettingsMenuItem(appUrls),
        getStatsMenuItem(appUrls),
        
        new separatorMenuItem('Server'),
        getDatabasesMenuItem(appUrls),
        rootItems.clusterDashboard(),
        getManageServerMenuItem(),
        rootItems.about(),
        rootItems.whatsNew(options),
    ];
}



