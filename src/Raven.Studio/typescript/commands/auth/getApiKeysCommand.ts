import commandBase = require("commands/commandBase");
import apiKey = require("models/auth/apiKey");

class getApiKeysCommand extends commandBase {
    
    execute(): JQueryPromise<Array<apiKey>> {
        //TODO: use new dedicated endpoint
        var args = {
            startsWith: "Raven/ApiKeys/",
            exclude: <string>null,
            start: 0,
            pageSize: 256
        };

        return this.query("/docs", args, null, (dtos: apiKeyDto[]) => dtos.map(dto => new apiKey(dto)));
    }
}

export = getApiKeysCommand;
