import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDocumentsMetadataByIDPrefixCommand extends commandBase {

    constructor(private prefix:string, private pageSize: number, private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<metadataAwareDto>> {
        const args = {
            startsWith: this.prefix,
            start: 0,
            pageSize: this.pageSize,
            metadataOnly: true
        };
        const url = endpoints.databases.document.docs + this.urlEncodeArgs(args);
        return this.query<Array<metadataAwareDto>>(url, null, this.db, x => x.Results);
    }
}

export = getDocumentsMetadataByIDPrefixCommand;
