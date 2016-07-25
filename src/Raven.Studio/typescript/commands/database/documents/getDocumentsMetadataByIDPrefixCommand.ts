import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getDocumentsMetadataByIDPrefixCommand extends commandBase {

    constructor(private prefix:string,private resultsAmount: number, private db: database) {
        super();
    }

    execute(): JQueryPromise<documentMetadataDto[]> {
        var url = '/docs';
        var args = {
            'startsWith': this.prefix,
            'exclude': <string> null,
            'start': 0,
            'pageSize': this.resultsAmount,
            'metadata-only': true
        };
        return this.query<any>(url, args, this.db);
    }
}

export = getDocumentsMetadataByIDPrefixCommand;
