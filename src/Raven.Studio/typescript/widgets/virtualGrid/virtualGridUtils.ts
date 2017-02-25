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

function escapeHtml(string: string) {
    return String(string).replace(/[&<>"'`=\/]/g, s => entityMap[s]);
}

function widthToPixels(column: virtualColumn) {
    if (!column.width.endsWith("px")) {
        throw new Error("Resize is only supported for columns with width specified in pixels");
    }
    return parseInt(column.width.slice(0, -2));
}

export = {
    escape: escapeHtml,
    widthToPixels: widthToPixels
};