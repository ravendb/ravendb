import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveDatabaseRecordCommand extends commandBase {
    private readonly db: database | string;
    private readonly databaseRecord: documentDto;
    private readonly etag: number;

    constructor(db: database | string, databaseRecord: documentDto, etag: number) {
        super();
        this.db = db;
        this.databaseRecord = databaseRecord;
        this.etag = etag;

        if (!db) {
            throw new Error("Must specify database");
        }
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminDatabases.adminDatabases;
        
        return this.put<void>(url, JSON.stringify(this.databaseRecord), null, { headers: { "ETag": this.etag?.toString() }})
            .done(() => this.reportSuccess("Database Record was saved successfully"))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Database Record", response.responseText, response.statusText));
    }
}

export = saveDatabaseRecordCommand;
