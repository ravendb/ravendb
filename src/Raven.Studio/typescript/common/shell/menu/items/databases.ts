import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getDatabasesMenuItem;

function getDatabasesMenuItem(appUrls: computedAppUrls) {
    return new leafMenuItem({
        route: ["", "databases"],
        title: "Resources",
        moduleId: "viewmodels/resources/databases",
        nav: true,
        css: 'icon-resources',
        dynamicHash: appUrls.databasesManagement
    });
}
