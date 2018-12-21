/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

interface sortableVirtualColumn extends virtualColumn {

    /**
     * Returns sort provider
     */
    sortProvider?(mode: sortMode): (array: Array<any>) => Array<any>;

    /**
     * Returns default sort order
     */
    defaultSortOrder?: null | "asc" | "desc";
}

export = sortableVirtualColumn;
