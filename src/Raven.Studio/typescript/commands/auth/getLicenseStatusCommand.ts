import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getLicenseStatusCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Commercial.LicenseStatus> {
        const url = endpoints.global.license.licenseStatus;
        return this.query(url, null);
    }
}

export = getLicenseStatusCommand;
