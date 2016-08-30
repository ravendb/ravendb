import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class createSampleDataClassCommand extends commandBase {
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<string> {
        return this.query<string>("/studio-tasks/createSampleDataClass", null, this.db);//TODO: use endpoints
     }
}

export = createSampleDataClassCommand;
