import commandBase = require("commands/commandBase");

class getLatestServerBuildVersionCommand extends commandBase {

    constructor(private stableOnly: boolean, private min: number, private max: number) {
        super();
    }

    execute(): JQueryPromise<serverBuildVersionDto> {
        var args = {
            stableOnly: this.stableOnly,
            min: this.min,
            max: this.max
        }

        return this.query("/studio-tasks/latest-server-build-version", args, null, null, this.getTimeToAlert(true));
    }
}

export = getLatestServerBuildVersionCommand;
