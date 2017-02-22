/// <reference path="../../../typings/tsd.d.ts"/>

import pagedResult = require("widgets/virtualGrid/pagedResult");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

interface virtualGridController<T> { 

    init(fetcher: (skip: number, take: number) => JQueryPromise<pagedResult<T>>, columnsProvider: (containerWidth:number, results: pagedResult<T>) => virtualColumn[]): void;

    headerVisible(value: boolean): void;

    reset(): void;

    getSelectedItems(): T[]; //TODO: refactor to list of selected indexes or 'all' selected except list of ids
}

export = virtualGridController;