import leafMenuItem = require("common/shell/menu/leafMenuItem");
import appUrl = require("common/appUrl");

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

function serverDashboard() {
    return new leafMenuItem({
        route: ["", "dashboard"],
        moduleId: 'viewmodels/resources/serverDashboard',
        title: 'Server dashboard',
        tooltip: "Server dashboard",
        nav: true,
        css: 'icon-dashboard',
        dynamicHash: appUrl.forServerDashboard
    });
}

export = {
    about: aboutItem,
    dashboard: serverDashboard
};
