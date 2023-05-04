import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class createSampleDataCommand extends commandBase {
    private db: database;

    constructor(db: database) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<any> {
        return this.post(endpoints.databases.sampleData.studioSampleData, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to create sample data", response.responseText, response.statusText))
            .done(() => this.reportSuccess("Sample data creation completed"));
    }
}

export = createSampleDataCommand;
