import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require('common/shell/menu/separatorMenuItem');

export = getTransformersMenuItem;

function getTransformersMenuItem(appUrls: computedAppUrls) {
    let transformersChildren = [
        new leafMenuItem({
            title: 'Transformers',
            route: 'databases/transformers',
            moduleId: 'viewmodels/database/transformers/transformers',
            css: 'icon-plus',
            nav: true,
            dynamicHash: appUrls.transformers
        }),
        new leafMenuItem({
            route: 'databases/transformers/edit(/:transformerName)',
            moduleId: 'viewmodels/database/transformers/editTransformer',
            title: 'Edit Transformer',
            css: 'icon-plus',
            nav: false
        }),
        new separatorMenuItem(),
        new leafMenuItem({
            title: 'Add Transformer',
            route: 'databases/transformers/add',
            moduleId: 'viewmodels/database/transformers/editTransformer',
            css: 'icon-plus',
            nav: true,
            dynamicHash: appUrls.newTransformer
        })
    ];

    return new intermediateMenuItem("Transformers", transformersChildren, 'icon-indexes');
}
