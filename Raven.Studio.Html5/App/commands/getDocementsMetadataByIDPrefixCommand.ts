/// <reference path="../models/dto.ts" />

import commandBase = require("commands/commandBase");
import database = require("models/database");

class getDocementsMetadataByIDPrefixCommand extends commandBase {

    constructor(private prefix:string,private resultsAmount: number, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var getDocsWithPrefixQueryPart = '/docs';
        var arguments =
        {
            'startsWith': this.prefix,
            'exclude': null,
            'start': 0,
            'pageSize': this.resultsAmount,
            'metadata-only': true
        };
        return this.query<any>(getDocsWithPrefixQueryPart, arguments, this.db, (results: documentMetadataDto[])=> {
            return results.map(d=> {
                return d['@metadata']['@id'];
            });
        });

    }


}

export = getDocementsMetadataByIDPrefixCommand;