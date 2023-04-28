import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class createSampleDataClassCommand extends commandBase {
    private db: database

    constructor(db: database) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<string> {
        return this.query<string>(endpoints.databases.sampleData.studioSampleDataClasses, null, this.db, null, { dataType: 'text' });
     }
}

export = createSampleDataClassCommand;
