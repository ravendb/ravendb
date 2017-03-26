/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualRow = require("widgets/virtualGrid/virtualRow");
import utils = require("widgets/virtualGrid/virtualGridUtils");

type actionColumnOpts<T> = {
    extraClass?: (item: T) => string;
    title?: (item:T) => string;
}
type provider<T> = (item: T) => string;

class actionColumn<T> implements virtualColumn {
    private readonly action: (obj: T) => void;

    header: string;
    private buttonText: (item: T) => string;
    width: string;
    extraClass: string;

    private opts: actionColumnOpts<T>;

    actionUniqueId = _.uniqueId("action-");

    constructor(action: (obj: T) => void, header: string, buttonText: provider<T> | string, width: string, opts: actionColumnOpts<T> = {}) {
        this.action = action;
        this.header = header;
        this.buttonText = _.isString(buttonText) ? (item: T) => buttonText : buttonText;
        this.width = width;
        this.opts = opts || {};
    }

    canHandle(actionId: string) {
        return this.actionUniqueId === actionId;
    }

    handle(row: virtualRow) {
        this.action(row.data as T);
    }

    renderCell(item: T, isSelected: boolean): string {
        const extraButtonHtml = this.opts.title ? ` title="${utils.escape(this.opts.title(item))}" ` : '';
        const extraCssClasses = this.opts.extraClass ? this.opts.extraClass(item) : '';
        return `<div class="cell action-cell" style="width: ${this.width}">
            <button type="button" ${extraButtonHtml} data-action="${this.actionUniqueId}" class="btn btn-sm btn-block ${extraCssClasses}">${this.buttonText(item)}</button>
        </div>`;
    }

}

export = actionColumn;