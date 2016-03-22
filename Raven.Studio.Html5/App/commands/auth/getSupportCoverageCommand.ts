import commandBase = require("commands/commandBase");

class getSupportCoverageCommand extends commandBase {

    execute(): JQueryPromise<supportCoverageDto> {
        return this.query("/license/support", null);
    }
}

export = getSupportCoverageCommand;
