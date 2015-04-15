import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class createSampleDataClassCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {

        return this.query<string>("/studio-tasks/createSampleDataClass", null, this.db);
     }
}

export = createSampleDataClassCommand;