import commandBase = require("commands/commandBase");

class getLicenseStatusCommand extends commandBase {

    execute(): JQueryPromise<licenseStatusDto> {
        return this.query("/license/status", null);//TODO: use endpoints
    }
}

export = getLicenseStatusCommand;
