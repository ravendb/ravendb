import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getQueryMenuItem;

function getQueryMenuItem(appUrls: computedAppUrls) {
    var routes: leafMenuItem[] = [
        new leafMenuItem({
            route: ['', 'databases/query/index(/:indexNameOrRecentQueryIndex)'],
            moduleId: 'viewmodels/database/query/query',
            title: 'Query',
            nav: true,
            css: 'icon-query',
            dynamicHash: appUrls.query('')
        }),
        /* TODO
        new leafMenuItem({
            route: 'databases/query/reporting(/:indexName)',
            moduleId: 'viewmodels/database/reporting/reporting',
            title: 'Reporting',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.reporting
        }),
        new leafMenuItem({
            route: 'databases/query/exploration',
            moduleId: 'viewmodels/database/exploration/exploration',
            title: "Data exploration",
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.exploration
        })*/
    ];

    return new intermediateMenuItem("Query", routes, 'icon-search');
}
