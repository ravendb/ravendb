import pagedResultSet = require("common/pagedResultSet");
import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getDocRefsCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private ownerDb: database, private docId:string, private skip: number, private take: number) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<pagedResultSet<any>> {
        
        var args = {
            id: this.docId,
            start: this.skip,
            pageSize: this.take
        };

        var url = "/debug/docrefs";//TODO: use endpoints
        var docRefsTask = $.Deferred();
        this.query<statusDebugDocrefsDto>(url, args, this.ownerDb).
            fail(response => docRefsTask.reject(response)).
            done((docRefs:statusDebugDocrefsDto) => {
                var items = $.map(docRefs.Results, r => {
                    return {
                        getId: () => r,
                        getDocumentPropertyNames: () => <Array<string>>["Id"]
                    }
                });
                var resultsSet = new pagedResultSet(items, docRefs.TotalCount);
                docRefsTask.resolve(resultsSet);
            });

        return docRefsTask;
    }
}

export = getDocRefsCommand;
