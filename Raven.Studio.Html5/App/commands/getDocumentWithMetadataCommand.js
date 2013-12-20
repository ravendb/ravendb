var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database", "models/document"], function(require, exports, commandBase, database, document) {
    var getDocumentWithMetadataCommand = (function (_super) {
        __extends(getDocumentWithMetadataCommand, _super);
        function getDocumentWithMetadataCommand(id, db) {
            _super.call(this);
            this.id = id;
            this.db = db;

            if (!id) {
                throw new Error("Must specify ID");
            }

            if (!db) {
                throw new Error("Must specify database");
            }
        }
        getDocumentWithMetadataCommand.prototype.execute = function () {
            var _this = this;
            // Executing /queries will return the doc with the metadata.
            // We can do a GET call to /docs/[id], however, it returns the metadata only as headers,
            // which can have some issues when querying via CORS.
            var documentResult = $.Deferred();
            var postResult = this.post("/queries", JSON.stringify([this.id]), this.db);
            postResult.fail(function (xhr) {
                return documentResult.fail(xhr);
            });
            postResult.done(function (queryResult) {
                if (queryResult.Results.length === 0) {
                    documentResult.reject("Unable to find document with ID " + _this.id);
                } else {
                    documentResult.resolve(new document(queryResult.Results[0]));
                }
            });

            return documentResult;
        };
        return getDocumentWithMetadataCommand;
    })(commandBase);

    
    return getDocumentWithMetadataCommand;
});
//# sourceMappingURL=getDocumentWithMetadataCommand.js.map
