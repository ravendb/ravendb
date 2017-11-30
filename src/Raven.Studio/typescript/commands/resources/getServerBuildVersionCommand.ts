import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerBuildVersionCommand extends commandBase {

    execute() {
        return this.query<serverBuildVersionDto>(endpoints.global.buildVersion.buildVersion, null);
    }
}

export = getServerBuildVersionCommand;
