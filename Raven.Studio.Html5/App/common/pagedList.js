/// <reference path="../../Scripts/typings/jquery/jquery.d.ts" />
/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />
/// <reference path="../../Scripts/extensions.ts" />
define(["require", "exports", "common/pagedResultSet"], function(require, exports, __pagedResultSet__) {
    var pagedResultSet = __pagedResultSet__;

    var pagedList = (function () {
        function pagedList(fetcher) {
            this.fetcher = fetcher;
            this.totalResultCount = ko.observable(0);
            this.items = [];
            this.isFetching = false;
            this.queuedFetch = null;
            this.collectionName = "";
            this.currentItemIndex = ko.observable(0);
            if (!fetcher) {
                throw new Error("Fetcher must be specified.");
            }
        }
        pagedList.prototype.fetch = function (skip, take) {
            var _this = this;
            if (this.isFetching) {
                this.queuedFetch = { skip: skip, take: take, task: $.Deferred() };
                return this.queuedFetch.task;
            }

            var cachedItemsSlice = this.getCachedSliceOrNull(skip, take);
            if (cachedItemsSlice) {
                // We've already fetched these items. Just return them immediately.
                var deferred = $.Deferred();
                var results = new pagedResultSet(cachedItemsSlice, this.totalResultCount());
                deferred.resolve(results);
                return deferred;
            } else {
                // We haven't fetched some of the items. Fetch them now from remote.
                this.isFetching = true;
                var remoteFetch = this.fetcher(skip, take).done(function (resultSet) {
                    _this.totalResultCount(resultSet.totalResultCount);
                    resultSet.items.forEach(function (r, i) {
                        return _this.items[i + skip] = r;
                    });
                }).always(function () {
                    _this.isFetching = false;
                    _this.runQueuedFetch();
                });
                return remoteFetch;
            }
        };

        pagedList.prototype.getCachedSliceOrNull = function (skip, take) {
            for (var i = skip; i < skip + take; i++) {
                if (!this.items[i]) {
                    return null;
                }
            }

            return this.items.slice(skip, skip + take);
        };

        pagedList.prototype.getNthItem = function (nth) {
            var deferred = $.Deferred();
            var cachedItemArray = this.getCachedSliceOrNull(nth, 1);
            if (cachedItemArray) {
                deferred.resolve(cachedItemArray[0]);
            } else {
                this.fetch(nth, 1).done(function (result) {
                    deferred.resolve(result.items[0]);
                }).fail(function (error) {
                    return deferred.reject(error);
                });
            }
            return deferred;
        };

        pagedList.prototype.getCachedItemsAt = function (indices) {
            var _this = this;
            return indices.filter(function (index) {
                return _this.items[index];
            }).map(function (validIndex) {
                return _this.items[validIndex];
            });
        };

        pagedList.prototype.runQueuedFetch = function () {
            if (this.queuedFetch) {
                var queuedSkip = this.queuedFetch.skip;
                var queuedTake = this.queuedFetch.take;
                var queuedTask = this.queuedFetch.task;
                this.queuedFetch = null;
                var fetchTask = this.fetch(queuedSkip, queuedTake);
                fetchTask.done(function (results) {
                    return queuedTask.resolve(results);
                });
                fetchTask.fail(function (error) {
                    return queuedTask.reject(error);
                });
            }
        };
        return pagedList;
    })();

    
    return pagedList;
});
//# sourceMappingURL=pagedList.js.map
