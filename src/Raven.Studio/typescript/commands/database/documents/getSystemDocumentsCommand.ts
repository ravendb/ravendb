import commandBase = require("commands/commandBase");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import pagedResultSet = require("common/pagedResultSet");

class getSystemDocumentsCommand extends commandBase {

    constructor(private db: database, private skip: number, private take: number, private totalResultCount: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet<any>> {

        // To get system docs, we just call /docs asking for docs that start with "Raven/".
        // Unfortunately, this returns a plain array; it doesn't tell how many *total* system docs there are.
        // This means we can't really do proper paging. 
        var args = {
            startsWith: "Raven/",
            exclude: <string>null,
            start: this.skip,
            pageSize: this.take
        };

        var deferred = $.Deferred();
        var docsQuery = this.query("/docs", args, this.db, (dtos: documentDto[]) => dtos.map(dto => new document(dto)));//TODO: use endpoints
        docsQuery.done((results: documentDto[]) => {
            var documents = results.map(dto => new document(dto));
            //var totalResultCount = documents.length; 
            var resultSet = new pagedResultSet(documents, this.totalResultCount);
            deferred.resolve(resultSet);
        });
        docsQuery.fail(response => deferred.reject(response));

        return deferred;
    }
}

export = getSystemDocumentsCommand;
