/// <reference path="../../../typings/tsd.d.ts"/>

import pagedResult = require("widgets/virtualGrid/pagedResult");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridSelection = require("widgets/virtualGrid/virtualGridSelection");

interface virtualGridController<T> { 

    init(fetcher: (skip: number, take: number) => JQueryPromise<pagedResult<T>>, columnsProvider: (containerWidth:number, results: pagedResult<T>) => virtualColumn[]): void;

    headerVisible(value: boolean): void;

    reset(): void;

    getSelection(): virtualGridSelection<T>;
}

export = virtualGridController;