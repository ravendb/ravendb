import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getDatabasesMenuItem;

function getDatabasesMenuItem(appUrls: computedAppUrls) {
    return new leafMenuItem({
        route: "databases",
        title: "Databases",
        moduleId: require("viewmodels/resources/databases"),
        nav: true,
        css: 'icon-resources',
        dynamicHash: appUrls.databasesManagement
    });
}
