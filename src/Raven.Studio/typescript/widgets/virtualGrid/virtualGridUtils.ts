/// <reference path="../../../typings/tsd.d.ts"/>

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

export = {
    escape: escapeHtml
};