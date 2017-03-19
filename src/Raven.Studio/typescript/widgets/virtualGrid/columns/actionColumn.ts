/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import document = require("models/database/documents/document");
import virtualRow = require("widgets/virtualGrid/virtualRow");

class actionColumn<T> implements virtualColumn {
    private readonly action: (obj: T) => void;

    header: string;
    private buttonText: string;
    width: string;

    actionUniqueId = _.uniqueId("action-");

    constructor(action: (obj: T) => void, header: string, buttonText: string, width: string) {
        this.action = action;
        this.header = header;
        this.buttonText = buttonText;
        this.width = width;
    }

    canHandle(actionId: string) {
        return this.actionUniqueId === actionId;
    }

    handle(row: virtualRow) {
        this.action(row.data as T);
    }

    renderCell(item: document, isSelected: boolean): string {
        return `<div class="cell action-cell" style="width: ${this.width}">
            <button data-action="${this.actionUniqueId}" class="btn btn-default btn-small">${this.buttonText}</button>
        </div>`;
    }

}

export = actionColumn;