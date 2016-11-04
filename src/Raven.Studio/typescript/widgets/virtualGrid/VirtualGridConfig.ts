import Column = require("widgets/virtualGrid/Column");
import PagedResult = require("widgets/virtualGrid/PagedResult");

interface VirtualGridConfig {

    /**
     * The function that fetches a chunk of items. This function will be invoked by the grid as the user scrolls and loads more items.
     */
    fetcher: (skip: number, take: number) => JQueryPromise<PagedResult>;

    /**
     * Optional. A list of columns to use. If not specified, the columns will be pulled from the first set of loaded items, with priority set on .Id and .Name columns.
     */
    columns?: Column[];
}

export = VirtualGridConfig;