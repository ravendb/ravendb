import commandBase = require("commands/commandBase");

class getServerBuildVersionCommand extends commandBase {

    execute(): JQueryPromise<serverBuildVersionDto> {
        return this.query("/build/version", null);
    }
}

export = getServerBuildVersionCommand;