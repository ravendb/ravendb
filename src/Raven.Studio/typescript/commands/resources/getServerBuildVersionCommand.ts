import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerBuildVersionCommand extends commandBase {

    execute() {
        const args = {
            t: new Date().getTime()
        };
        return this.query<serverBuildVersionDto>(endpoints.global.buildVersion.buildVersion, args);
    }
}

export = getServerBuildVersionCommand;
