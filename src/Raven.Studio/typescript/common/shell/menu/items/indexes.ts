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
            css: 'icon-indexing',
            dynamicHash: appUrls.indexes
        }),
        new leafMenuItem({
            route: 'databases/status/indexing',
            moduleId: 'viewmodels/database/status/indexing/indexPerformance',  //TODO: move viewmodel/view to proper directory
            title: 'Indexing performance',
            tooltip: "Shows details about indexing peformance",
            nav: true,
            css: 'icon-index-batch-size',
            dynamicHash: appUrls.indexPerformance
        }),
        new leafMenuItem({
            route: 'databases/status/indexing/stats',
            moduleId: 'viewmodels/database/status/indexing/indexStats', //TODO: move viewmodel/view to proper directory
            title: 'Index stats',
            tooltip: "Show details about indexing in/out counts",
            nav: true,
            css: 'icon-index-stats',
            dynamicHash: appUrls.indexStats
        }),
        /* TODO
        new leafMenuItem({
            title: "Merge suggestions",
            nav: true,
            route: "databases/indexes/mergeSuggestions",
            moduleId: "viewmodels/database/indexes/indexMergeSuggestions",
            css: 'icon-merge',
            dynamicHash: appUrls.megeSuggestions
        }),*/
        new leafMenuItem({
            title: 'Edit Index',
            route: 'databases/indexes/edit(/:indexName)',
            moduleId: 'viewmodels/database/indexes/editIndex',
            css: 'icon-edit',
            nav: false
        }),
        new leafMenuItem({
            title: 'Terms',
            route: 'databases/indexes/terms/(:indexName)',
            moduleId: 'viewmodels/database/indexes/indexTerms',
            css: 'icon-terms',
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

    return new intermediateMenuItem("Indexes", indexesChildren, 'icon-indexing');
}
