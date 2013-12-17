var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/document", "models/database", "common/pagedResultSet"], function(require, exports, commandBase, document, database, pagedResultSet) {
    var getSystemDocumentsCommand = (function (_super) {
        __extends(getSystemDocumentsCommand, _super);
        function getSystemDocumentsCommand(db, skip, take) {
            _super.call(this);
            this.db = db;
            this.skip = skip;
            this.take = take;
        }
        getSystemDocumentsCommand.prototype.execute = function () {
            // To get system docs, we just call /docs asking for docs that start with "Raven/".
            // Unfortunately, this returns a plain array; it doesn't tell how many *total* system docs there are.
            // This means we can't really do proper paging.
            var args = {
                startsWith: "Raven/",
                exclude: null,
                start: this.skip,
                pageSize: this.take
            };

            var deferred = $.Deferred();
            var docsQuery = this.query("/docs", args, this.db, function (dtos) {
                return dtos.map(function (dto) {
                    return new document(dto);
                });
            });
            docsQuery.done(function (results) {
                var documents = results.map(function (dto) {
                    return new document(dto);
                });
                var totalResultCount = documents.length;
                var resultSet = new pagedResultSet(documents, totalResultCount);
                deferred.resolve(resultSet);
            });
            docsQuery.fail(function (response) {
                return deferred.reject(response);
            });

            return deferred;
        };
        return getSystemDocumentsCommand;
    })(commandBase);

    
    return getSystemDocumentsCommand;
});
//# sourceMappingURL=getSystemDocumentsCommand.js.map
