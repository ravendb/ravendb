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
            css: 'fa fa-dashboard',
            dynamicHash: appUrls.resourcesManagement
        }),
        new separatorMenuItem(),
        new leafMenuItem({
            route: [""],
            title: "New database",
            moduleId: "viewmodels/resources/resources",
            nav: true,
            css: 'icon-resources',
        }),
        new leafMenuItem({
            route: [""],
            title: "New filesystem",
            moduleId: "viewmodels/resources/resources",
            nav: true,
            css: 'icon-resources',
        }),
        new leafMenuItem({
            route: [""],
            title: "New counter",
            moduleId: "viewmodels/resources/resources",
            nav: true,
            css: 'icon-resources',
        }),
        new leafMenuItem({
            route: [""],
            title: "New time serie",
            moduleId: "viewmodels/resources/resources",
            nav: true,
            css: 'icon-resources',
        })
    ];

    return new intermediateMenuItem("Resources", items, "icon-resources");
}
