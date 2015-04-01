import commandBase = require("commands/commandBase");

class getLatestServerBuildVersionCommand extends commandBase {

    constructor(private stableOnly: boolean = true) {
        super();
    }

    execute(): JQueryPromise<serverBuildVersionDto> {
        var args = {
            stableOnly: this.stableOnly
        }

        return this.query("/studio-tasks/latest-server-build-version", args, null, null, this.getTimeToAlert(true));
    }
}

export = getLatestServerBuildVersionCommand;