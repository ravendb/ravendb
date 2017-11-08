import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import collectionMenuItem = require("common/shell/menu/collectionMenuItem");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

export = getDocumentsMenuItem;

function getDocumentsMenuItem(appUrls: computedAppUrls) {
    let documentsChildren = [
        new leafMenuItem({
            title: "List of documents",
            nav: false,
            route: "databases/documents",
            moduleId: "viewmodels/database/documents/documents",
            css: 'icon-documents',
            dynamicHash: appUrls.documents
        }),

        new leafMenuItem({
            title: "Revisions Bin",
            nav: false,
            route: "databases/documents/revisions/bin",
            moduleId: "viewmodels/database/documents/revisionsBin",
            css: 'icon-revisions-bin',
            dynamicHash: appUrls.revisionsBin
        }),

        new collectionMenuItem(),
        new leafMenuItem({
            title: "Patch",
            nav: true,
            route: "databases/patch(/:recentPatchHash)",
            moduleId: "viewmodels/database/patch/patch",
            css: 'icon-patch',
            dynamicHash: appUrls.patch
        }),
        new leafMenuItem({
            title: "Conflicts",
            nav: true,
            route: "databases/documents/conflicts",
            moduleId: "viewmodels/database/conflicts/conflicts",
            css: 'icon-conflicts',
            dynamicHash: appUrls.conflicts,
            badgeData: collectionsTracker.default.conflictsCount
        }),
        new leafMenuItem({
            route: "databases/edit",
            title: "Edit Document",
            moduleId: "viewmodels/database/documents/editDocument",
            nav: false,
            itemRouteToHighlight: "databases/documents"
        })
    ];

    return new intermediateMenuItem("Documents", documentsChildren, "icon-documents", {
        dynamicHash: appUrls.documents
    });
}
