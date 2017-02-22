/// <reference path="../../../typings/tsd.d.ts"/>

import pagedResult = require("widgets/virtualGrid/pagedResult");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

class virtualGridConfig<T> {

    /**
     * Whether to show the header containing the column names.
     */
    showHeader = ko.observable<boolean>(true);

    /**
     * Whether to show the row selection checkbox column. Defaults to true. If so, this will be the first column in the grid.
     */
    showRowSelectionCheckbox = true;

    /**
     * Optional. A list of columns to use. If not specified, the columns will be pulled from the first set of loaded items, with priority set on .Id and .Name columns.
     */
    customColumns: virtualColumn[] = null;

    /**
     * The function that fetches a chunk of items. This function will be invoked by the grid as the user scrolls and loads more items.
     */
    fetcher: (skip: number, take: number) => JQueryPromise<pagedResult<T>>;
}

export = virtualGridConfig;