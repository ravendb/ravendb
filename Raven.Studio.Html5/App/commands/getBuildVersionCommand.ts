import commandBase = require("commands/commandBase");

class getBuildVersionCommand extends commandBase {

    execute(): JQueryPromise<buildVersionDto> {
        return this.query("/build/version", null);
    }
}

export = getBuildVersionCommand;