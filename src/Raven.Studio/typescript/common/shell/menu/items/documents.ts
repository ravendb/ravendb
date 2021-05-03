import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import collectionMenuItem = require("common/shell/menu/collectionMenuItem");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import accessManager = require("common/shell/accessManager");

export = getDocumentsMenuItem;

const access = accessManager.default.databaseDocumentsMenu;

function getDocumentsMenuItem(appUrls: computedAppUrls) {
    let documentsItems = [
        new leafMenuItem({
            title: "List of documents",
            nav: false,
            route: "databases/documents",
            moduleId: "viewmodels/database/documents/documents",
            css: "icon-documents",
            dynamicHash: appUrls.documents
        }),
        new leafMenuItem({
            title: "Revisions Bin",
            nav: false,
            route: "databases/documents/revisions/bin",
            moduleId: "viewmodels/database/documents/revisionsBin",
            css: "icon-revisions-bin",
            dynamicHash: appUrls.revisionsBin
        }),
        new collectionMenuItem(),
        new leafMenuItem({
            title: "Patch",
            nav: true,
            route: "databases/patch(/:recentPatchHash)",
            moduleId: "viewmodels/database/patch/patch",
            css: "icon-patch",
            dynamicHash: appUrls.patch,
            disableWithReason: access.disablePatchMenuItem,
            requiredAccess: "DatabaseReadWrite"
        }),
        new leafMenuItem({
            route: "databases/query/index(/:indexNameOrRecentQueryIndex)",
            moduleId: "viewmodels/database/query/query",
            title: "Query",
            nav: true,
            css: "icon-documents-query",
            alias: true,
            dynamicHash: appUrls.query("")
        }),
        new leafMenuItem({
            title: "Conflicts",
            nav: true,
            route: "databases/documents/conflicts",
            moduleId: "viewmodels/database/conflicts/conflicts",
            css: "icon-conflicts",
            dynamicHash: appUrls.conflicts,
            badgeData: collectionsTracker.default.conflictsCount
        }),
        new leafMenuItem({
            route: "databases/edit",
            title: "Edit Document",
            moduleId: "viewmodels/database/documents/editDocument",
            nav: false,
            itemRouteToHighlight: "databases/documents"
        }),
        new leafMenuItem({
            route: "databases/ts/edit",
            title: "Edit Time Series",
            moduleId: "viewmodels/database/timeSeries/editTimeSeries",
            nav: false,
            itemRouteToHighlight: "databases/documents"
        }),
        new leafMenuItem({
            route: "databases/cmpXchg/edit",
            title: "Edit Compare Exchange Value",
            moduleId: "viewmodels/database/cmpXchg/editCmpXchg",
            nav: false,
            itemRouteToHighlight: "databases/cmpXchg"
        }),
        new leafMenuItem({
            title: "Compare Exchange",
            nav: true,
            route: "databases/cmpXchg",
            moduleId: "viewmodels/database/cmpXchg/cmpXchg",
            css: "icon-cmp-xchg",
            dynamicHash: appUrls.cmpXchg
        })
    ];

    return new intermediateMenuItem("Documents", documentsItems, "icon-documents", {
        dynamicHash: appUrls.documents
    });
}
