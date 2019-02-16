/// <reference path="../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import generalUtils = require("common/generalUtils");

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


function nl2br(str: string) {
    return str = str.replace(/(?:\r\n|\r|\n)/g, '<br>');
}

function widthToPixels(column: virtualColumn) {
    if (!column.width.endsWith("px")) {
        throw new Error("Excepted column width in pixels (px)");
    }
    return parseInt(column.width.slice(0, -2));
}

export = {
    escape: generalUtils.escapeHtml,
    widthToPixels,
    deselect,
    nl2br
};
