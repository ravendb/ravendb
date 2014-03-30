/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import pagedResultSet = require("common/pagedResultSet");

class pagedList {

    totalResultCount = ko.observable(0);
    private items = [];
    isFetching = false;
    queuedFetch: { skip: number; take: number; task: JQueryDeferred<pagedResultSet> } = null;
    collectionName = "";
    currentItemIndex = ko.observable(0);

    constructor(private fetcher: (skip: number, take: number) => JQueryPromise<pagedResultSet>) {
        if (!fetcher) {
            throw new Error("Fetcher must be specified.");
        }
    }

    fetch(skip: number, take: number): JQueryPromise<pagedResultSet> {
        if (this.isFetching) {
            this.queuedFetch = { skip: skip, take: take, task: $.Deferred() };
            return this.queuedFetch.task;
        }

        var cachedItemsSlice = this.getCachedSliceOrNull(skip, take);
        if (cachedItemsSlice) {
            // We've already fetched these items. Just return them immediately.
            var deferred = $.Deferred<pagedResultSet>();
            var results = new pagedResultSet(cachedItemsSlice, this.totalResultCount());
            deferred.resolve(results);
            return deferred;
        }
        else {
            // We haven't fetched some of the items. Fetch them now from remote.
            this.isFetching = true;
            var remoteFetch = this.fetcher(skip, take)
                .done((resultSet: pagedResultSet) => {
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

        return this.items.slice(skip, skip + take)
    }

    getNthItem(nth: number): JQueryPromise<any> {
        var deferred = $.Deferred();
        var cachedItemArray = this.getCachedSliceOrNull(nth, 1);
        if (cachedItemArray) {
            deferred.resolve(cachedItemArray[0]);
        } else {
            this.fetch(nth, 1)
                .done((result: pagedResultSet) => {
                    deferred.resolve(result.items[0]);
                })
                .fail(error => deferred.reject(error));
        }
        return deferred;
    }

    getCachedItemsAt(indices: number[]): any[] {
        return indices
            .filter(index => this.items[index])
            .map(validIndex => this.items[validIndex]);
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

}

export = pagedList;