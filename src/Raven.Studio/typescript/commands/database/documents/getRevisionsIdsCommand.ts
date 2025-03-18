import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

interface RevisionsIdsResult {
    Results: {
        Id: string;
    }[];
}

export default class getRevisionsIdsCommand extends commandBase {
    private readonly databaseName: string;
    private readonly prefix: string;
    private readonly pageSize: number;

    constructor(databaseName: string, prefix: string, pageSize: number) {
        super();
        this.databaseName = databaseName;
        this.prefix = prefix;
        this.pageSize = pageSize;
    }

    execute(): JQueryPromise<RevisionsIdsResult> {
        const args = {
            prefix: this.prefix,
            pageSize: this.pageSize,
        };

        const url = endpoints.databases.studioCollections.studioRevisionsIds;

        return this.query<RevisionsIdsResult>(url, args, this.databaseName);
    }
}
