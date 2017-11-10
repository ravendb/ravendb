import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class checkIfServerIsOnlineCommand extends commandBase {

    constructor(private url: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.setup.setupAlive;

        return this.query<void>(url, null, null, null,  { dataType: undefined },  null, this.url);
    }
}

export = checkIfServerIsOnlineCommand;
