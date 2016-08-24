/// <reference path="../../typings/tsd.d.ts" />

import pagedResultSet = require("common/pagedResultSet");

class pagedList {

    totalResultCount = ko.observable(0);
    private items: Array<any> = [];
    isFetching = false;
    queuedFetch: { skip: number; take: number; task: JQueryDeferred<pagedResultSet<any>> } = null;
    collectionName = "";
    currentItemIndex = ko.observable(0);

    constructor(private fetcher: fetcherDto<any>) {
        if (!fetcher) {
            throw new Error("Fetcher must be specified.");
        }
    }

    clear() {
        if (!!this.queuedFetch) {
            this.queuedFetch.task.reject("data is being reloaded");
            this.queuedFetch = null;
        }

        while (this.items.length > 0) {
            this.items.pop();
        }
    }

    itemCount(): number {
        return this.items ? this.items.length : 0;
    }

    fetch(skip: number, take: number): JQueryPromise<pagedResultSet<any>> {
        if (this.isFetching) {
            this.queuedFetch = { skip: skip, take: take, task: $.Deferred() };
            return this.queuedFetch.task;
        }

        var cachedItemsSlice = this.getCachedSliceOrNull(skip, take);
        if (cachedItemsSlice) {
            // We've already fetched these items. Just return them immediately.
            var deferred = $.Deferred<pagedResultSet<any>>();
            var results = new pagedResultSet(cachedItemsSlice, this.totalResultCount());
            deferred.resolve(results);
            return deferred;
        }
        else {
            // We haven't fetched some of the items. Fetch them now from remote.
            this.isFetching = true;
            var remoteFetch = this.fetcher(skip, take)
                .done((resultSet: pagedResultSet<any>) => {
                    this.totalResultCount(resultSet.totalResultCount);
                    resultSet.items.forEach((r, i) => this.items[i + skip] = r);
                })
                .always(() => {
                    this.isFetching = false;
                    this.runQueuedFetch();
                });
            return remoteFetch;
        }
    }

    getCachedSliceOrNull(skip: number, take: number): any[] {
        for (var i = skip; i < skip + take; i++) {
            if (!this.items[i]) {
                return null;
            }
        }

        return this.items.slice(skip, skip + take);
    }

    getNthItem(nth: number): JQueryPromise<any> {
        var deferred = $.Deferred();
        var cachedItemArray = this.getCachedSliceOrNull(nth, 1);
        if (cachedItemArray) {
            deferred.resolve(cachedItemArray[0]);
        } else {
            this.fetch(nth, 1)
                .done((result: pagedResultSet<any>) => {
                    deferred.resolve(result.items[0]);
                })
                .fail((error: any) => deferred.reject(error));
        }
        return deferred;
    }

    getCachedItemsAt(indices: number[]): any[] {
        return indices
            .filter(index => this.items[index])
            .map(validIndex => this.items[validIndex]);
    }

    getCachedIndices(indices: number[]): number[] {
        return indices.filter(index => this.items[index]);
    }

    getAllCachedItems(): any[] {
        return this.items;
    }

    runQueuedFetch() {
        if (this.queuedFetch) {
            var queuedSkip = this.queuedFetch.skip;
            var queuedTake = this.queuedFetch.take;
            var queuedTask = this.queuedFetch.task;
            this.queuedFetch = null;
            var fetchTask = this.fetch(queuedSkip, queuedTake);
            fetchTask.done(results => queuedTask.resolve(results));
            fetchTask.fail(error => queuedTask.reject(error));
        }
    }

    invalidateCache() {
        this.items.length = 0;
    }

    indexOf(item: any) {
        return this.items.indexOf(item);
    }

    hasIds(): boolean {
        return this.items && this.items.length > 0 && this.items[0] && this.items[0].getId && this.items[0].getId();
    }

    getItem(i: number) {
        if (i > this.items.length - 1)
            return null;
        return this.items[0];
    }
}

export = pagedList;
