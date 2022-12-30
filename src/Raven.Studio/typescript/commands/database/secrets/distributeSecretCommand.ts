import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class distributeSecretCommand extends commandBase {

    private name: string;

    private secret: string;

    private nodeTags: string[];

    constructor(name: string, secret: string, nodeTags: string[]) {
        super();
        this.nodeTags = nodeTags;
        this.secret = secret;
        this.name = name;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name,
            node: this.nodeTags
        };
        const url = endpoints.global.secretKey.adminSecretsDistribute + this.urlEncodeArgs(args);
        
        return this.post<void>(url, this.secret, null, { dataType: 'text' })
            .fail((response: JQueryXHR) => this.reportError("Failed to distribute secret key", response.responseText, response.statusText));
    }
}

export = distributeSecretCommand;
