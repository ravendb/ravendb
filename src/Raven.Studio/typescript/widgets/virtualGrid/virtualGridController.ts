/// <reference path="../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridSelection = require("widgets/virtualGrid/virtualGridSelection");

interface virtualGridController<T> { 

    init(fetcher: (skip: number, take: number) => JQueryPromise<pagedResult<T>>, columnsProvider: (containerWidth:number, results: pagedResult<T>) => virtualColumn[]): void;

    headerVisible(value: boolean): void;

    reset(hard?: boolean): void;

    selection: KnockoutObservable<virtualGridSelection<T>>;

    getSelectedItems(): T[];

    setSelectedItems(selection: Array<T>): void;

    dirtyResults: KnockoutObservable<boolean>;

    resultEtag: () => string;
}

export = virtualGridController;