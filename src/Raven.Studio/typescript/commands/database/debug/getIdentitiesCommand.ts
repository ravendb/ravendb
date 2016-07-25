import pagedResultSet = require("common/pagedResultSet");
import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIdentitiesCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private ownerDb: database, private skip: number, private take: number) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<pagedResultSet<statusDebugIdentitiesDto>> {
        
        var args = {
            start: this.skip,
            pageSize: this.take
        };

        var url = "/debug/identities";
        var identitiesTask = $.Deferred();
        this.query<statusDebugIdentitiesDto>(url, args, this.ownerDb).
            fail(response => identitiesTask.reject(response)).
            done((identities: statusDebugIdentitiesDto) => {
                var items = $.map(identities.Identities, r => { 
                    return {
                        getId: () => r.Key,
                        getUrl: () => r.Key,
                        'Value': r.Value,
                        'Key': r.Key,
                        getDocumentPropertyNames: () => <Array<string>>["Key", "Value"]
                    }
                });
                var resultsSet = new pagedResultSet(items, identities.TotalCount);
                identitiesTask.resolve(resultsSet);
            });

        return identitiesTask;
    }
}

export = getIdentitiesCommand;
