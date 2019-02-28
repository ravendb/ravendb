/// <reference path="../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

function deselect() {
    try {
        if ((document as any).selection) {
            // Timeout neccessary for IE9					
            setTimeout(function () {
                (document as any).selection.empty();
            });
        } else {
            window.getSelection().removeAllRanges();
        }
    } catch (err) {
    }
}

function widthToPixels(column: virtualColumn) {
    if (!column.width.endsWith("px")) {
        throw new Error("Excepted column width in pixels (px)");
    }
    return parseInt(column.width.slice(0, -2));
}

export = {
    widthToPixels: widthToPixels,
    deselect: deselect
};
