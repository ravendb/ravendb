import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

function getReplicationMenuItem(appUrls: computedAppUrls) {
    const replicationChildren = [
        new leafMenuItem({
            title: "Conflicts",
            nav: true,
            route: "databases/replicationEtl/conflicts",
            moduleId: "viewmodels/database/conflicts/conflicts",
            css: 'icon-plus',
            dynamicHash: appUrls.conflicts
        })

        //TODO: move all other replication and etl related items to this menu
    ];

    return new intermediateMenuItem("Replication & ETL", replicationChildren, "icon-replication");
}

export = getReplicationMenuItem;