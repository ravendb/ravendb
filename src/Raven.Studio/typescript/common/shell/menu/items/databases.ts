import leafMenuItem = require("common/shell/menu/leafMenuItem");
import appUrl from "common/appUrl";

export = getDatabasesMenuItem;

function getDatabasesMenuItem(appUrls: computedAppUrls) {
    const databasesView = require("viewmodels/resources/databases");
    
    appUrl.defaultModule = databasesView;
    
    return new leafMenuItem({
        route: "databases",
        title: "Databases",
        moduleId: databasesView,
        nav: true,
        css: 'icon-resources',
        dynamicHash: appUrls.databasesManagement
    });
}
