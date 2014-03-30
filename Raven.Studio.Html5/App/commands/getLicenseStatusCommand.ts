import commandBase = require("commands/commandBase");
import database = require("models/database");

class getLicenseStatusCommand extends commandBase {

    execute(): JQueryPromise<licenseStatusDto> {
        return this.query("/license/status", null);
    }
}

export = getLicenseStatusCommand;