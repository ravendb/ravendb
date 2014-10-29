/// <reference path="../models/dto.ts" />

import commandBase = require("commands/commandBase");
import database = require("models/database");
import transformer = require("models/transformer");
 
class deleteTransformerCommand extends commandBase {

    constructor(private transName: string, private db:database) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Deleting " + this.transName + "...");
        return this.del("/transformers/" + this.transName, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to delete transformer " + this.transName, response.responseText))
            .done(() => this.reportSuccess("Deleted " + this.transName));
    }


}

 export = deleteTransformerCommand;