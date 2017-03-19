/// <reference path="../../../../../typings/tsd.d.ts" />
import leafMenuItem = require("common/shell/menu/leafMenuItem");
export = getTransformersMenuItems;

function getTransformersMenuItems(appUrls: computedAppUrls) {
    let transformersChildren = [
        new leafMenuItem({
            title: 'Transformers',
            route: 'databases/transformers',
            moduleId: 'viewmodels/database/transformers/transformers',
            css: 'icon-etl',
            nav: true,
            dynamicHash: appUrls.transformers,
        }),
        new leafMenuItem({
            route: 'databases/transformers/edit(/:transformerName)',
            moduleId: 'viewmodels/database/transformers/editTransformer',
            title: 'Edit Transformer',
            css: 'icon-edit',
            nav: false,
            itemRouteToHighlight: 'databases/transformers'
        })
    ];

    return transformersChildren;
}
