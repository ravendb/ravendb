/// <reference path="../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

const entityMap: any = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
    '/': '&#x2F;',
    '`': '&#x60;',
    '=': '&#x3D;'
};

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

function escapeHtml(string: string) {
    return String(string).replace(/[&<>"'`=\/]/g, s => entityMap[s]);
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
    escape: escapeHtml,
    widthToPixels,
    deselect,
    nl2br
};
