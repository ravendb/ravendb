define(["require", "exports"], function(require, exports) {
    var pagedResultSet = (function () {
        function pagedResultSet(items, totalResultCount) {
            this.items = items;
            this.totalResultCount = totalResultCount;
        }
        return pagedResultSet;
    })();

    
    return pagedResultSet;
});
//# sourceMappingURL=pagedResultSet.js.map
