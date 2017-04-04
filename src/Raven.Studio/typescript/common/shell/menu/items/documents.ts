import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import collectionMenuItem = require("common/shell/menu/collectionMenuItem");

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

        new collectionMenuItem(),

/* TODO:
        new leafMenuItem({
            title: "Conflicts",
            nav: true,
            route: "database/conflicts",
            moduleId: "viewmodels/database/conflicts/conflicts",
            css: 'icon-plus',
            dynamicHash: appUrls.conflicts
        }),*/
        new leafMenuItem({
            title: "Patch",
            nav: true,
            route: "databases/patch(/:recentPatchHash)",
            moduleId: "viewmodels/database/patch/patch",
            css: 'icon-patch',
            dynamicHash: appUrls.patch
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
