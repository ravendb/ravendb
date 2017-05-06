import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class generateSecretCommand extends commandBase {

    execute(): JQueryPromise<string> {
        const url = endpoints.global.secretKey.adminSecretsGenerate;

        return this.query(url, null, null, null, { dataType: 'text' });
    }
}

export = generateSecretCommand;
