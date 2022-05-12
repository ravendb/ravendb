import leafMenuItem = require("common/shell/menu/leafMenuItem");
import appUrl = require("common/appUrl");
import accessManager = require("common/shell/accessManager");

function aboutItem() {
    return new leafMenuItem({
        route: 'about',
        moduleId: require('viewmodels/shell/about'),
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
        moduleId: require('viewmodels/resources/serverDashboard'),
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
        moduleId: require('viewmodels/resources/clusterDashboard'),
        title: 'Cluster Dashboard',
        tooltip: "Cluster Dashboard",
        nav: true, // todo - this needs issue RavenDB-16618 to work...
        css: 'icon-cluster-dashboard',
        dynamicHash: appUrl.forClusterDashboard
    }); 
}

export = {
    about: aboutItem,
    clusterDashboard,
    dashboard: serverDashboard,
};
