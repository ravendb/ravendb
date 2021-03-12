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
        title: 'Server Dashboard',
        tooltip: "Server Dashboard",
        nav: true,
        css: 'icon-dashboard',
        dynamicHash: appUrl.forServerDashboard
    });
}

function clusterDashboard() {
    return new leafMenuItem({
        route: "clusterDashboard",
        moduleId: 'viewmodels/resources/clusterDashboard',
        title: 'Cluster Dashboard',
        tooltip: "Cluster Dashboard",
        nav: true,
        css: 'icon-cluster-dashbaord',
        dynamicHash: appUrl.forClusterDashboard
    }); 
}

export = {
    about: aboutItem,
    clusterDashboard,
    dashboard: serverDashboard,
};
