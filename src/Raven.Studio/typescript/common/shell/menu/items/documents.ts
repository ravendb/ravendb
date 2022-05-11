import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import collectionMenuItem = require("common/shell/menu/collectionMenuItem");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

export = getDocumentsMenuItem;

function getDocumentsMenuItem(appUrls: computedAppUrls) {
    let documentsItems = [
        new leafMenuItem({
            title: "All Documents",
            nav: false,
            route: "databases/documents",
            moduleId: require("viewmodels/database/documents/documents"),
            css: "icon-documents",
            dynamicHash: appUrls.documents
        }),
        new leafMenuItem({
            title: "Revisions Bin",
            nav: false,
            route: "databases/documents/revisions/bin",
            moduleId: require("viewmodels/database/documents/revisionsBin"),
            css: "icon-revisions-bin",
            dynamicHash: appUrls.revisionsBin
        }),
        new collectionMenuItem(),
        new leafMenuItem({
            title: "Patch",
            nav: true,
            route: "databases/patch(/:recentPatchHash)",
            moduleId: require("viewmodels/database/patch/patch"),
            css: "icon-patch",
            dynamicHash: appUrls.patch,
            requiredAccess: "DatabaseReadWrite"
        }),
        new leafMenuItem({
            route: "databases/query/index(/:indexNameOrRecentQueryIndex)",
            moduleId: require("viewmodels/database/query/query"),
            title: "Query",
            nav: true,
            css: "icon-documents-query",
            alias: true,
            dynamicHash: appUrls.query("")
        }),
        new leafMenuItem({
            route: "databases/edit",
            title: "Edit Document",
            moduleId: require("viewmodels/database/documents/editDocument"),
            nav: false,
            itemRouteToHighlight: "databases/documents"
        }),
        new leafMenuItem({
            route: "databases/ts/edit",
            title: "Edit Time Series",
            moduleId: require("viewmodels/database/timeSeries/editTimeSeries"),
            nav: false,
            itemRouteToHighlight: "databases/documents"
        }),
        new leafMenuItem({
            title: "Identities",
            nav: true,
            route: "databases/identities",
            moduleId: require("viewmodels/database/identities/identities"),
            css: "icon-identities",
            dynamicHash: appUrls.identities
        }),
        new leafMenuItem({
            title: "Compare Exchange",
            nav: true,
            route: "databases/cmpXchg",
            moduleId: require("viewmodels/database/cmpXchg/cmpXchg"),
            css: "icon-cmp-xchg",
            dynamicHash: appUrls.cmpXchg
        }),
        new leafMenuItem({
            route: "databases/cmpXchg/edit",
            title: "Edit Compare Exchange Value",
            moduleId: require("viewmodels/database/cmpXchg/editCmpXchg"),
            nav: false,
            itemRouteToHighlight: "databases/cmpXchg"
        }),
        new leafMenuItem({
            title: "Conflicts",
            nav: true,
            route: "databases/documents/conflicts",
            moduleId: require("viewmodels/database/conflicts/conflicts"),
            css: "icon-conflicts",
            dynamicHash: appUrls.conflicts,
            badgeData: collectionsTracker.default.conflictsCount
        })
    ];

    return new intermediateMenuItem("Documents", documentsItems, "icon-documents", {
        dynamicHash: appUrls.documents
    });
}
