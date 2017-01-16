import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getResourcesMenuItem;

function getResourcesMenuItem(appUrls: computedAppUrls) {
    return new leafMenuItem({
        route: ["", "resources"],
        title: "Resources",
        moduleId: "viewmodels/resources/resources",
        nav: true,
        css: 'icon-resources',
        dynamicHash: appUrls.resourcesManagement
    });
}
