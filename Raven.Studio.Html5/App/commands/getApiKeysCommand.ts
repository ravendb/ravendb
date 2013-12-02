import commandBase = require("commands/commandBase");
import apiKey = require("models/apiKey");

class getApiKeysCommand extends commandBase {
    
    execute(): JQueryPromise<Array<apiKey>> {
        var args = {
            startsWith: "Raven/ApiKeys/",
            exclude: null,
            start: 0,
            pageSize: 256
        };

        return this.query("/docs", args, null, (dto: apiKeyDto) => new apiKey(dto));
    }
}

export = getApiKeysCommand;