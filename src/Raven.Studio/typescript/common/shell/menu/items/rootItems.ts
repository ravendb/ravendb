import leafMenuItem = require("common/shell/menu/leafMenuItem");
import appUrl = require("common/appUrl");
import { bridgeToReact } from "common/reactUtils";
import { BootstrapPlaygroundPage } from "components/pages/BootstrapPlaygroundPage";
import { AboutPage } from "components/pages/resources/about/AboutPage";
import React from "react";

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

interface WhatsNewItemOptions {
    isNewVersionAvailable?: boolean;
    isWhatsNewVisible?: boolean;
}

function whatsNewItem({ isNewVersionAvailable = false, isWhatsNewVisible = false }: WhatsNewItemOptions = {}) {
    
    const moduleId = bridgeToReact(
        () => React.createElement(AboutPage, { initialChangeLogMode: "changeLog" }),
        "nonShardedView"
    );

    const badgeHtml = isNewVersionAvailable
        ? `<div class="badge badge-info rounded-pill">Update available</div>`
        : null

    return new leafMenuItem({
        route: 'whatsNew',
        moduleId,
        title: 'What\'s new',
        tooltip: "What's new",
        nav: isWhatsNewVisible,
        css: 'icon-sparkles',
        dynamicHash: appUrl.forWhatsNew,
        badgeHtml
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
