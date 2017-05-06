import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class putSecretCommand extends commandBase {

    constructor(private name: string, private secret: string, private overwrite: boolean = false) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name,
            overwrite: this.overwrite
        }
        const url = endpoints.global.secretKey.adminSecrets + this.urlEncodeArgs(args);

        return this.put<void>(url, this.secret, null, { dataType: 'text' });
    }
}

export = putSecretCommand;
