/// <reference path="../../../typings/tsd.d.ts"/>

import pagedResult = require("widgets/virtualGrid/pagedResult");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

class virtualGridConfig<T> {
    /**
     * Whether to show the header containing the column names.
     */
    showHeader = ko.observable<boolean>(true);

    /**
     * The function to calculate columns based on sample data
     */
    columnsProvider: (containerWidth:number, results: pagedResult<T>) => virtualColumn[];

    /**
     * The function that fetches a chunk of items. This function will be invoked by the grid as the user scrolls and loads more items.
     */
    fetcher: (skip: number, take: number) => JQueryPromise<pagedResult<T>>;
}

export = virtualGridConfig;