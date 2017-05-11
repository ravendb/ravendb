/// <reference path="../../../../typings/tsd.d.ts" />

import appUrl = require("common/appUrl");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import database = require("models/resources/database");

import getManageServerMenuItem = require("common/shell/menu/items/manageServer");
import getDatabasesMenuItem = require("common/shell/menu/items/databases");
import getStatsMenuItem = require("common/shell/menu/items/stats");
import getSettingsMenuItem = require("common/shell/menu/items/settings");
import getTasksMenuItem = require("common/shell/menu/items/tasks");
import getTransformersMenuItems = require("common/shell/menu/items/transformers");
import getIndexesMenuItem = require("common/shell/menu/items/indexes");
import getReplicationMenuItem = require("common/shell/menu/items/replication");
import getDocumentsMenuItem = require("common/shell/menu/items/documents");

export = generateMenuItems;

function generateMenuItems(db: database) {
    if (!db) {
        return generateNoActiveDatabaseMenuItems();
    } 

    return generateActiveDatabaseMenuItems();
}

function aboutItem() {
    return new leafMenuItem({
        route: 'about',
        moduleId: 'viewmodels/shell/about',
        title: 'About',
        tooltip: "About",
        nav: true,
        css: 'icon-info',
        dynamicHash: appUrl.forAbout
    });
}

function generateNoActiveDatabaseMenuItems() {
    const appUrls = appUrl.forCurrentDatabase();
    return [
        new separatorMenuItem('Server'),
        getDatabasesMenuItem(appUrls),
        getManageServerMenuItem(),
        aboutItem()
    ];
    
}

function generateActiveDatabaseMenuItems() {
    const appUrls = appUrl.forCurrentDatabase();
    const transformersItems = getTransformersMenuItems(appUrls);
    return [
        getDocumentsMenuItem(appUrls),
        getIndexesMenuItem(appUrls),
        ...transformersItems,
        getReplicationMenuItem(appUrls),
        getTasksMenuItem(appUrls),
        getSettingsMenuItem(appUrls),
        getStatsMenuItem(appUrls),
        new separatorMenuItem('Server'),
        getDatabasesMenuItem(appUrls),
        getManageServerMenuItem(),
        aboutItem()
    ];
}



