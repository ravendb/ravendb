/// <reference path="../../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import virtualGridUtils = require("widgets/virtualGrid/virtualGridUtils");
import tempStatDialog = require("viewmodels/database/status/indexing/tempStatDialog");
import app = require("durandal/app");

type documentBasedColumnsProviderOpts = {
    showRowSelectionCheckbox?: boolean;
    enableInlinePreview?: boolean;
    showSelectAllCheckbox?: boolean;
}

class documentBasedColumnsProvider {

    private static readonly minColumnWidth = 150;

    private readonly showRowSelectionCheckbox: boolean;
    private readonly collectionNames: string[];
    private readonly db: database;
    private readonly enableInlinePreview: boolean;
    private readonly showSelectAllCheckbox: boolean;

    private static readonly externalIdRegex = /^\w+\/\w+/ig;

    constructor(db: database, collectionNames: string[], opts: documentBasedColumnsProviderOpts) {
        this.showRowSelectionCheckbox = _.isBoolean(opts.showRowSelectionCheckbox) ? opts.showRowSelectionCheckbox : false;
        this.collectionNames = collectionNames;
        this.db = db;
        this.enableInlinePreview = _.isBoolean(opts.enableInlinePreview) ? opts.enableInlinePreview : false;
        this.showSelectAllCheckbox = _.isBoolean(opts.showSelectAllCheckbox) ? opts.showSelectAllCheckbox : false;
    }

    findColumns(viewportWidth: number, results: pagedResult<document>): virtualColumn[] {
        const columnNames = this.findColumnNames(results, Math.floor(viewportWidth / documentBasedColumnsProvider.minColumnWidth));

        // Insert the row selection checkbox column as necessary.
        const initialColumns: virtualColumn[] = [];

        if (this.showRowSelectionCheckbox) {
            initialColumns.push(new checkedColumn(this.showSelectAllCheckbox));
        }

        if (this.enableInlinePreview) {
            const previewColumn = new actionColumn<document>((doc: document) => this.showPreview(doc), "Preview", `<span class="icon-search"></span>`, "70px");
            initialColumns.push(previewColumn);
        }

        const initialColumnsWidth = _.sumBy(initialColumns, x => virtualGridUtils.widthToPixels(x));
        const rightScrollWidth = 5;
        const remainingSpaceForOtherColumns = viewportWidth - initialColumnsWidth - rightScrollWidth;
        const columnWidth = Math.floor(remainingSpaceForOtherColumns / columnNames.length) + "px";

        return initialColumns.concat(columnNames.map(p => {
            if (p === "__metadata") {
                return new hyperlinkColumn((x: document) => x.getId(), x => appUrl.forEditDoc(x.getId(), this.db), "Id", columnWidth);
            }

            return new hyperlinkColumn(p, _.partial(this.findLink, _, p).bind(this), p, columnWidth);
        }));
    }

    //TODO: is this class right place for this?
    private showPreview(doc: document) {
        app.showBootstrapDialog(new tempStatDialog(doc));
    }

    private findColumnNames(results: pagedResult<document>, limit: number): string[] {
        const uniquePropertyNames = new Set<string>();

        results.items
            .map(i => _.keys(i).forEach(key => uniquePropertyNames.add(key)));

        const columnNames = Array.from(uniquePropertyNames);

        if (columnNames.length > limit) {
            columnNames.length = limit;
        }

        // Put Id and Name columns first.
        const prioritizedColumns = ["__metadata", "Name"];
        prioritizedColumns
            .reverse()
            .forEach(c => {
                const columnIndex = columnNames.indexOf(c);
                if (columnIndex >= 0) {
                    columnNames.splice(columnIndex, 1);
                    columnNames.unshift(c);
                }
            });

        return columnNames;
    }

    private findLink(item: document, property: string): string {
        if (property in item) {
            const value = (item as any)[property];

            //TODO: support url's in data as well
            if (_.isString(value) && value.match(documentBasedColumnsProvider.externalIdRegex)) {
                const extractedCollectionName = value.split("/")[0].toLowerCase();
                const matchedCollection = this.collectionNames.find(x => extractedCollectionName.startsWith(x.toLowerCase()));
                return matchedCollection ? appUrl.forEditDoc(value, this.db) : null;
            }
        }
        return null;
    }
}

export = documentBasedColumnsProvider;