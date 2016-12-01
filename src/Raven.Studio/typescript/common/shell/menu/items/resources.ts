import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getResourcesMenuItem;

function getResourcesMenuItem(appUrls: computedAppUrls) {
    var items = [
        new leafMenuItem({
            route: ["", "resources"],
            title: "Dashboard",
            moduleId: "viewmodels/resources/resources",
            nav: true,
            css: 'icon-dashboard',
            dynamicHash: appUrls.resourcesManagement
        })
        //TODO: new fs, new cs, new ts
       
    ];

    return new intermediateMenuItem("Resources", items, "icon-resources");
}
