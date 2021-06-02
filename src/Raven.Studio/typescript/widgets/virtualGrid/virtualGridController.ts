/// <reference path="../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualRow = require("widgets/virtualGrid/virtualRow");
import virtualGridSelection = require("widgets/virtualGrid/virtualGridSelection");

interface virtualGridController<T> { 

    init(fetcher: (skip: number, pageSize: number) => JQueryPromise<pagedResult<T>>, columnsProvider: (containerWidth:number, results: pagedResult<T>) => virtualColumn[]): void;

    markColumnsDirty: () => void;
    
    headerVisible(value: boolean): void;

    customRowClassProvider(provider: (item: T) => string[]): void;

    reset(hard?: boolean, retainSort?: boolean): void;

    findRowForCell(cellElement: JQuery | Element): virtualRow;

    findItem(predicate: (item: T, idx: number) => boolean): T;

    selection: KnockoutObservable<virtualGridSelection<T>>;

    getSelectedItems(): T[];

    setSelectedItems(selection: Array<T>): void;

    dirtyResults: KnockoutObservable<boolean>;

    resultEtag: () => string;
    
    scrollDown: () => void;

    setDefaultSortBy(columnIndex: number, mode?: sortMode): void;
}

export = virtualGridController;
