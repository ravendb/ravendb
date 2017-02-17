/// <reference path="../../../../../typings/tsd.d.ts"/>

import pagedResult = require("widgets/virtualGrid/pagedResult");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");

class documentBasedProvider<T> { //TODO: do we want generic here?

    private readonly showRowSelectionCheckbox: boolean;
    private readonly db: database;

    constructor(db: database, showRowSelectionCheckbox: boolean) {
        this.showRowSelectionCheckbox = showRowSelectionCheckbox;
        this.db = db;
    }

    findColumns(viewportWidth: number, results: pagedResult<T>): virtualColumn[] {
        //TODO: refactor
        const propertySet = {};
        const uniquePropertyNames = new Set<string>();

        const items = results.items;
        items.map(i => _.keys(i).forEach(key => uniquePropertyNames.add(key)));

        const columnNames = Array.from(uniquePropertyNames);
        const checkedColumnWidth = this.showRowSelectionCheckbox ? checkedColumn.columnWidth : 0;
        const columnWidth = Math.floor(viewportWidth / columnNames.length) - checkedColumnWidth + "px";

        // Put Id and Name columns first.
        const prioritizedColumns = ["Id", "Name"];
        prioritizedColumns
            .forEach(c => {
                const columnIndex = columnNames.indexOf(c);
                if (columnIndex >= 0) {
                    columnNames.splice(columnIndex, 1);
                    columnNames.unshift(c);
                }
            });

        // Insert the row selection checkbox column as necessary.
        const initialColumns: virtualColumn[] = this.showRowSelectionCheckbox ? [new checkedColumn()] : [];
        return initialColumns.concat(columnNames.map(p => {
            if (p === "__metadata") {
                return new hyperlinkColumn((x: any) => x.getId(), x => appUrl.forEditDoc(x.getId(), this.db), "Id", columnWidth);
            }

            return new textColumn((obj: any) => obj[p], p, columnWidth);
        }));
    }
}

export = documentBasedProvider;