import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import collectionMenuItem = require("common/shell/menu/collectionMenuItem");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

export = getDocumentsMenuItem;

function getDocumentsMenuItem(appUrls: computedAppUrls) {
    const documentsItems = [
        new leafMenuItem({
            route: "databases/documents",
            moduleId: require("viewmodels/database/documents/documents"),
            shardingMode: "allShards",
            title: "All Documents",
            nav: false,
            css: "icon-documents",
            dynamicHash: appUrls.documents
        }),
        new leafMenuItem({
            route: "databases/documents/revisions/bin",
            moduleId: require("viewmodels/database/documents/revisionsBin"),
            shardingMode: "allShards",
            title: "Revisions Bin",
            nav: false,
            css: "icon-revisions-bin",
            dynamicHash: appUrls.revisionsBin
        }),
        new collectionMenuItem(),
        new leafMenuItem({
            route: "databases/patch(/:recentPatchHash)",
            moduleId: require("viewmodels/database/patch/patch"),
            shardingMode: "allShards",
            title: "Patch",
            nav: true,
            css: "icon-patch",
            dynamicHash: appUrls.patch,
            requiredAccess: "DatabaseReadWrite"
        }),
        new leafMenuItem({
            route: "databases/query/index(/:indexNameOrRecentQueryIndex)",
            moduleId: require("viewmodels/database/query/query"),
            shardingMode: "allShards",
            title: "Query",
            nav: true,
            css: "icon-documents-query",
            alias: true,
            dynamicHash: appUrls.query("")
        }),
        new leafMenuItem({
            route: "databases/edit",
            moduleId: require("viewmodels/database/documents/editDocument"),
            shardingMode: "allShards",
            title: "Edit Document",
            nav: false,
            itemRouteToHighlight: "databases/documents"
        }),
        new leafMenuItem({
            route: "databases/ts/edit",
            moduleId: require("viewmodels/database/timeSeries/editTimeSeries"),
            title: "Edit Time Series",
            nav: false,
            itemRouteToHighlight: "databases/documents"
        }),
        new leafMenuItem({
            route: "databases/identities",
            moduleId: require("viewmodels/database/identities/identities"),
            shardingMode: "allShards",
            title: "Identities",
            nav: true,
            css: "icon-identities",
            dynamicHash: appUrls.identities,
            search: {
                innerActions: [
                    { name: "Add new identity" }
                ],
            },
        }),
        new leafMenuItem({
            route: "databases/cmpXchg",
            moduleId: require("viewmodels/database/cmpXchg/cmpXchg"),
            shardingMode: "allShards",
            title: "Compare Exchange",
            nav: true,
            css: "icon-cmp-xchg",
            dynamicHash: appUrls.cmpXchg
        }),
        new leafMenuItem({
            route: "databases/cmpXchg/edit",
            moduleId: require("viewmodels/database/cmpXchg/editCmpXchg"),
            shardingMode: "allShards",
            title: "Edit Compare Exchange Value",
            nav: false,
            itemRouteToHighlight: "databases/cmpXchg"
        }),
        new leafMenuItem({
            route: "databases/documents/conflicts",
            moduleId: require("viewmodels/database/conflicts/conflicts"),
            shardingMode: "allShards",
            title: "Conflicts",
            nav: true,
            css: "icon-conflicts",
            dynamicHash: appUrls.conflicts,
            badgeData: collectionsTracker.default.conflictsCount
        })
    ];

    return new intermediateMenuItem("Documents", documentsItems, "icon-documents", {
        dynamicHash: appUrls.documents
    });
}
