/// <reference path="../models/dto.ts" />

import commandBase = require("commands/commandBase");
import database = require("models/database");

class getDocumentsMetadataByIDPrefixCommand extends commandBase {

    constructor(private prefix:string,private resultsAmount: number, private db: database) {
        super();
    }

    execute(): JQueryPromise<documentMetadataDto[]> {
        var url = '/docs';
        var arguments = {
            'startsWith': this.prefix,
            'exclude': null,
            'start': 0,
            'pageSize': this.resultsAmount,
            'metadata-only': true
        };
        return this.query<any>(url, arguments, this.db);
    }
}

export = getDocumentsMetadataByIDPrefixCommand;