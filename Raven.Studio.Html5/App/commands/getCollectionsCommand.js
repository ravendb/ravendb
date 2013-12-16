var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database", "models/collection"], function(require, exports, commandBase, database, collection) {
    var createDatabaseCommand = (function (_super) {
        __extends(createDatabaseCommand, _super);
        /**
        * @param ownerDb The database the collections will belong to.
        */
        function createDatabaseCommand(ownerDb) {
            _super.call(this);
            this.ownerDb = ownerDb;

            if (!this.ownerDb) {
                throw new Error("Must specify a database.");
            }
        }
        createDatabaseCommand.prototype.execute = function () {
            var _this = this;
            var args = {
                field: "Tag",
                fromValue: "",
                pageSize: 128
            };

            var resultsSelector = function (collectionNames) {
                return collectionNames.map(function (n) {
                    return new collection(n, _this.ownerDb);
                });
            };
            return this.query("/terms/Raven/DocumentsByEntityName", args, this.ownerDb, resultsSelector);
        };
        return createDatabaseCommand;
    })(commandBase);

    
    return createDatabaseCommand;
});
//# sourceMappingURL=getCollectionsCommand.js.map
