import commandBase = require("commands/commandBase");

class getServerConfigsCommand extends commandBase {

    execute(): JQueryPromise<serverConfigsDto> {
        return this.query("/studio-tasks/server-configs", null);//TODO: use endpoints
    }
}

export = getServerConfigsCommand;
