import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class createSampleDataCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.post(endpoints.databases.sampleData.studioSampleData, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to create sample data", response.responseText, response.statusText))
            .done(() => this.reportSuccess("Sample data creation completed"));
    }
}

export = createSampleDataCommand;
