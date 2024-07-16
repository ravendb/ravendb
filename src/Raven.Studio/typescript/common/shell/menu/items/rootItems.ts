import leafMenuItem = require("common/shell/menu/leafMenuItem");
import appUrl = require("common/appUrl");
import { bridgeToReact } from "common/reactUtils";
import { BootstrapPlaygroundPage } from "components/pages/BootstrapPlaygroundPage";
import { AboutPage } from "components/pages/resources/about/AboutPage";

function aboutItem() {
    return new leafMenuItem({
        route: 'about',
        moduleId: bridgeToReact(AboutPage, "nonShardedView"),
        title: 'About',
        tooltip: "About",
        nav: true,
        css: 'icon-info',
        dynamicHash: appUrl.forAbout,
        search: {
            innerActions: [
                {
                    name: "License",
                    alternativeNames: [
                        "Renew license",
                        "Replace license",
                        "Upgrade license",
                        "Force update license",
                        "License details",
                    ],
                },
                { name: "Version", alternativeNames: ["Server version", "Software version"] },
                { name: "Check for updates" },
                { name: "Support" },
                { name: "Send Feedback" },
            ],
        },
    });
}

function whatsNewItem() {
    return new leafMenuItem({
        route: 'whatsNew',
        moduleId: bridgeToReact(AboutPage, "nonShardedView"),
        title: 'What\'s new',
        tooltip: "What's new",
        nav: true,
        css: 'icon-sparkles',
        dynamicHash: appUrl.forAbout
    });
}

function bs5Item() {
    return new leafMenuItem({
        route: 'bs5',
        moduleId: bridgeToReact(BootstrapPlaygroundPage, "nonShardedView"),
        title: 'Bootstrap 5',
        tooltip: "Boostrap 5",
        nav: false,
        css: 'icon-info',
        dynamicHash: () => "#bs5",
        search: {
            isExcluded: true
        }
    });
}


function clusterDashboard() {
    const clusterDashboardView = require('viewmodels/resources/clusterDashboard');
    
    appUrl.clusterDashboardModule = clusterDashboardView;
    
    return new leafMenuItem({
        route: ["", "clusterDashboard"],
        moduleId: clusterDashboardView,
        title: 'Cluster Dashboard',
        tooltip: "Cluster Dashboard",
        nav: true, // todo - this needs issue RavenDB-16618 to work...
        css: 'icon-cluster-dashboard',
        dynamicHash: appUrl.forClusterDashboard,
        search: {
            innerActions: [
                { name: "Add widgets to board" },
                { name: "Delete widget from board", alternativeNames: ["Remove widget from board"] },
                { name: "Maximize widget" },
            ],
        },
    });
}

export = {
    about: aboutItem,
    whatsNew: whatsNewItem,
    bs: bs5Item,
    clusterDashboard
};
