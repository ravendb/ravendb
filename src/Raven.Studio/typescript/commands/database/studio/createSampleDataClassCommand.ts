import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class createSampleDataClassCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<string> {
        return this.query<string>(endpoints.databases.sampleData.studioSampleDataClasses, null, this.db, null, { dataType: 'text' });
     }
}

export = createSampleDataClassCommand;
