import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getIdentitiesCommand extends commandBase {

    constructor(private ownerDb: database, private skip: number, private take: number) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<pagedResult<statusDebugIdentitiesDto>> {
        
        var args = {
            start: this.skip,
            pageSize: this.take
        };

        var url = "/debug/identities";//TODO: use endpoints
        var identitiesTask = $.Deferred<pagedResult<any>>();
        this.query<statusDebugIdentitiesDto>(url, args, this.ownerDb).
            fail(response => identitiesTask.reject(response)).
            done((identities: statusDebugIdentitiesDto) => {
                var items = identities.Identities.map(r => { 
                    return {
                        getId: () => r.Id,
                        getUrl: () => r.Id,
                        'Value': r.Value,
                        'Key': r.Id,
                        getDocumentPropertyNames: () => <Array<string>>["Key", "Value"]
                    };
                });
                identitiesTask.resolve({ items: items, totalResultCount: identities.TotalCount });
            });

        return identitiesTask;
    }
}

export = getIdentitiesCommand;
