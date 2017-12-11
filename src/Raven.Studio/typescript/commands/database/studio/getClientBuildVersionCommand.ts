import commandBase = require("commands/commandBase");

class getClientBuildVersionCommand extends commandBase {

    execute(): JQueryPromise<clientBuildVersionDto> {
        const args = {
            t: new Date().getTime()
        };
        return this.query("/studio/version.json", args);
    }
}

export = getClientBuildVersionCommand;
