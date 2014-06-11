import commandBase = require("commands/commandBase");
import database = require("models/database");

class createSampleDataClassCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Important: SampleData Classes are updated if database wasn't changed");
        
        return this.query<string>("/studio-tasks/createSampleDataClass", null, this.db)
            .fail((response: JQueryXHR)=> this.reportError("Failed to create sample data classes", response.responseText, response.statusText));
           // .done(() => this.reportSuccess("Sample data creation completed"));
    }
}

export = createSampleDataClassCommand;