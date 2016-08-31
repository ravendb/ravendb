import commandBase = require("commands/commandBase");

class getClientBuildVersionCommand extends commandBase {

    execute(): JQueryPromise<clientBuildVersionDto> {
        return this.query("/studio/version.json", null);//TODO: use endpoints
    }
}

export = getClientBuildVersionCommand;
