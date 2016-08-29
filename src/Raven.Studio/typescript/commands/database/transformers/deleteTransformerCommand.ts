import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class deleteTransformerCommand extends commandBase {

    constructor(private transName: string, private db:database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Deleting " + this.transName + "...");
        return this.del("/transformers/" + this.transName, null, this.db)//TODO: use endpoints
            .fail((response: JQueryXHR) => this.reportError("Failed to delete transformer " + this.transName, response.responseText))
            .done(() => this.reportSuccess("Deleted " + this.transName));
    }


}

 export = deleteTransformerCommand;
