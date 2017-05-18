import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class distributeSecretCommand extends commandBase {

    constructor(private name: string, private secret: string, private nodeTags: string[]) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name,
            node: this.nodeTags
        }
        const url = endpoints.global.secretKey.adminSecretsDistribute + this.urlEncodeArgs(args);
        
        return this.post<void>(url, this.secret, null, { dataType: 'text' });
    }
}

export = distributeSecretCommand;
