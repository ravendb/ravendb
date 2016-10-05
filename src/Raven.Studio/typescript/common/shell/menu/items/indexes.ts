import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require('common/shell/menu/separatorMenuItem');

export = getIndexesMenuItem;

function getIndexesMenuItem(appUrls: computedAppUrls) {
    let indexesChildren = [
        new leafMenuItem({
            title: "List of indexes",
            nav: true,
            route: "databases/indexes",
            moduleId: "viewmodels/database/indexes/indexes",
            css: 'icon-list',
            dynamicHash: appUrls.indexes
        }),
        new leafMenuItem({
            title: "Merge suggestions",
            nav: true,
            route: "databases/indexes/mergeSuggestions",
            moduleId: "viewmodels/database/indexes/indexMergeSuggestions",
            css: 'icon-merge',
            dynamicHash: appUrls.megeSuggestions
        }),
        new leafMenuItem({
            title: 'Edit Index',
            route: 'databases/indexes/edit(/:indexName)',
            moduleId: 'viewmodels/database/indexes/editIndex',
            css: 'icon-plus',
            nav: false
        }),
        new leafMenuItem({
            title: 'Terms',
            route: 'databases/indexes/terms/(:indexName)',
            moduleId: 'viewmodels/database/indexes/indexTerms',
            css: 'icon-plus',
            nav: false
        }),
        new separatorMenuItem(),
        new leafMenuItem({
            title: 'Add Index',
            route: 'databases/indexes/add',
            moduleId: 'viewmodels/database/indexes/editIndex',
            css: 'icon-plus',
            nav: true,
            dynamicHash: appUrls.newIndex
        })
    ];

    return new intermediateMenuItem("Indexes", indexesChildren, 'icon-indexes');
}
