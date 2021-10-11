import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class seedIdentityCommand extends commandBase {

    constructor(private db: database, private prefix: string, private value: number) {
        super();
    }

    execute(): JQueryPromise<dictionary<number>> {
        const args = {
            name: this.prefix,
            value: this.value,
            force: true
        };
        
        const url = endpoints.databases.identity.identitySeed + this.urlEncodeArgs(args);

        return this.post(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(`Failed to set identity`, response.responseText, response.statusText));
    }
}

export = seedIdentityCommand;
