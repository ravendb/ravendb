import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDocumentsByIDPrefixCommand extends commandBase {

    constructor(private prefix:string, private pageSize: number, private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Array<metadataAwareDto>> {
        const args = {
            startsWith: this.prefix,
            start: 0,
            pageSize: this.pageSize,
            metadataOnly: false
        };
        const url = endpoints.databases.document.docs + this.urlEncodeArgs(args);
        return this.query<Array<documentDto>>(url, null, this.db, x => x.Results);
    }
}

export = getDocumentsByIDPrefixCommand;
