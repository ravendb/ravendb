define(["require", "exports"], function(require, exports) {
    var indexes = (function () {
        function indexes() {
            this.indexGroups = ko.observableArray();
            // TODO: fill this with real data.
            this.indexGroups.pushAll([
                { name: 'FakeIndexGroup1' },
                { name: 'FakeIndexGroup2' }
            ]);
        }
        indexes.prototype.activate = function () {
        };

        indexes.prototype.navigateToQuery = function () {
            console.log("TODO: implement");
        };

        indexes.prototype.navigateToNewIndex = function () {
            console.log("TODO: implement");
        };

        indexes.prototype.collapseAll = function () {
            console.log("TODO: implement");
        };

        indexes.prototype.expandAll = function () {
            console.log("TODO: implement");
        };
        return indexes;
    })();

    
    return indexes;
});
//# sourceMappingURL=indexes.js.map
