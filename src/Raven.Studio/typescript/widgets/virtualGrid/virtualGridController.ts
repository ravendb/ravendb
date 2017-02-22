/// <reference path="../../../typings/tsd.d.ts"/>

import pagedResult = require("widgets/virtualGrid/pagedResult");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

interface virtualGridController<T> { 

    init(fetcher: (skip: number, take: number) => JQueryPromise<pagedResult<T>>): void;

    headerVisible(value: boolean): void;

    rowSelectionCheckboxVisible(value: boolean): void;

    useColumns(columns: virtualColumn[]): void;

    useDefaultColumns(): void;

    reset(): void;
}

export = virtualGridController;