/// <reference path="../../typings/tsd.d.ts" />

import resource = require("models/resources/resource");

class cssGenerator {
    static getCssClass(entityName: string, colorMaps: resourceStyleMap[], rs: resource): string {
        var resourceStyleMap = colorMaps.first((map: resourceStyleMap) => map.resourceName === rs.name);
        if (!resourceStyleMap) {
            resourceStyleMap = {
                resourceName: rs.name,
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
