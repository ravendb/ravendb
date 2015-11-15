import executeBulkDocsCommand = require("commands/executeBulkDocsCommand");
import database = require("models/database");
import commandBase = require("commands/commandBase");

import deleteDocumentCommand = require("commands/deleteDocumentCommand");

class updateServerPrefixHiLoCommand extends commandBase {

    constructor(private serverPrefix: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {     
        if (this.serverPrefix) {

            var doc = {
                ServerPrefix: this.serverPrefix
            };
            return this.put("/docs/Raven/ServerPrefixForHilo", JSON.stringify(doc), this.db)
                .done(() => this.reportSuccess("Saved ServerPrefix configuration"))
                .fail((response: JQueryXHR) => this.reportError("Failed to save ServerPrefix configuration", response.responseText, response.statusText));
            
        } else {
            return new deleteDocumentCommand("Raven/ServerPrefixForHilo", this.db)
                .execute()
                .done(() => this.reportSuccess("Saved ServerPrefix configuration"))
                .fail((response: JQueryXHR) => this.reportError("Failed to save ServerPrefix configuration", response.responseText, response.statusText));
        }   
    }

}

export = updateServerPrefixHiLoCommand;
