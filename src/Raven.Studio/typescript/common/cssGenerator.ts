/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");

class cssGenerator {
    static getCssClass(entityName: string, colorMaps: resourceStyleMap[], db: database): string {
        var resourceStyleMap = colorMaps.find((map: resourceStyleMap) => map.resourceName === db.name);
        if (!resourceStyleMap) {
            resourceStyleMap = {
                resourceName: db.name,
                styleMap: {}
            };
            colorMaps.push(resourceStyleMap);
        }

        var existingStyle = resourceStyleMap.styleMap[entityName];
        if (existingStyle) {
            return existingStyle;
        }

        // We don't have an existing style. Assign one in the form of 'collection-style-X', where X is a number between 0 and maxStyleCount. These styles are found in app.less.
        var maxStyleCount = 32;
        var styleNumber = Object.keys(resourceStyleMap.styleMap).length % maxStyleCount;
        var style = "collection-style-" + styleNumber;
        resourceStyleMap.styleMap[entityName] = style;
        return style;
    }
}

export = cssGenerator;
