import commandBase = require("commands/commandBase");

class getServerBuildVersionCommand extends commandBase {

    execute() {
        return this.query<serverBuildVersionDto>("/build/version", null);//TODO: use endpoints
    }
}

export = getServerBuildVersionCommand;
