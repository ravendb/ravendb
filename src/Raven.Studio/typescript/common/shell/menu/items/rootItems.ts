import leafMenuItem = require("common/shell/menu/leafMenuItem");
import appUrl = require("common/appUrl");
import reactUtils = require("common/reactUtils");
import AboutPage = require("components/pages/resources/about/AboutPage");
import React = require("react");

function aboutItem() {
    return new leafMenuItem({
        route: 'about',
        moduleId: reactUtils.bridgeToReact(AboutPage.AboutPage, "nonShardedView"),
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

interface WhatsNewItemOptions {
    isWhatsNewVisible?: boolean;
}

function whatsNewItem({ isWhatsNewVisible = false }: WhatsNewItemOptions = {}) {
    
    const moduleId = reactUtils.bridgeToReact(
        () => React.createElement(AboutPage.AboutPage, { initialChangeLogMode: "changeLog" }),
        "nonShardedView"
    );

    return new leafMenuItem({
        route: 'whatsNew',
        moduleId,
        title: 'Release notes',
        tooltip: "Release notes",
        nav: isWhatsNewVisible,
        css: 'icon-sparkles',
        dynamicHash: appUrl.forWhatsNew,
        search: {
            isExcluded: !isWhatsNewVisible,
            isCapitalizedDisabled: true
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
    clusterDashboard
};
