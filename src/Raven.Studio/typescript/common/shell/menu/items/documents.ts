import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getDocumentsMenuItem;

function getDocumentsMenuItem(appUrls: computedAppUrls) {
    let documentsChildren = [
        new leafMenuItem({
            title: "Documents",
            nav: true,
            route: "databases/documents",
            moduleId: "viewmodels/database/documents/documents",
            dynamicHash: appUrls.documents,
            css: 'icon-plus'
        }),
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
            css: 'icon-plus',
            dynamicHash: appUrls.patch
        }),
        new leafMenuItem({
            route: "databases/edit",
            title: "Edit Document",
            moduleId: "viewmodels/database/documents/editDocument",
            nav: false
        })
    ];

    return new intermediateMenuItem("Documents", documentsChildren, "icon-documents");
}
