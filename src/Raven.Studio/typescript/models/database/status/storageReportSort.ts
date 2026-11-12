/// <reference path="../../../../typings/tsd.d.ts"/>

import storageReportItem = require("models/database/status/storageReportItem");
import genUtils = require("common/generalUtils");

export type storageReportSortColumn = "type" | "name" | "pageCount" | "numberOfEntries" | "dataSize" | "tempBuffersSize" | "size";

export class storageReportSort {

    column = ko.observable<storageReportSortColumn>("size");
    mode = ko.observable<sortMode>("desc");

    sortBy = (column: storageReportSortColumn) => {
        if (this.column() === column) {
            this.mode(this.mode() === "asc" ? "desc" : "asc");
            return;
        }

        this.column(column);
        this.mode(storageReportSort.isTextColumn(column) ? "asc" : "desc");
    };

    cssClassFor(column: storageReportSortColumn) {
        return this.column() === column ? this.mode() : "";
    }

    ariaSortFor(column: storageReportSortColumn) {
        if (this.column() !== column) {
            return "none";
        }

        return this.mode() === "asc" ? "ascending" : "descending";
    }

    reset() {
        this.column("size");
        this.mode("desc");
    }

    apply(items: storageReportItem[]): storageReportItem[] {
        const column = this.column();
        const mode = this.mode();

        return [...items].sort((a, b) => storageReportSort.isTextColumn(column)
            ? genUtils.sortAlphaNumeric(a[column], b[column], mode)
            : storageReportSort.compareValues(a[column], b[column], mode));
    }

    private static isTextColumn(column: storageReportSortColumn): column is "type" | "name" {
        return column === "type" || column === "name";
    }

    private static compareValues(a: number, b: number, mode: sortMode) {
        if (a == null || b == null) {
            return (a == null ? 1 : 0) - (b == null ? 1 : 0);
        }

        return mode === "asc" ? a - b : b - a;
    }
}
