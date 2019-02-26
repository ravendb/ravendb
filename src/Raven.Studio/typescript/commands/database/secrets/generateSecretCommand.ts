import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class generateSecretCommand extends commandBase {

    execute(): JQueryPromise<string> {
        const url = endpoints.global.secretKey.secretsGenerate;

        return this.query<string>(url, null, null, null, { dataType: 'text' })
            .fail((response: JQueryXHR) => this.reportError("Failed to generate secrets", response.responseText, response.statusText));
    }
}

export = generateSecretCommand;
