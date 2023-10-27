import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import LicenseLeaseResult = Raven.Server.Commercial.LicenseLeaseResult;
import messagePublisher = require("common/messagePublisher");

class forceLicenseUpdateCommand extends commandBase {
    execute(): JQueryPromise<LicenseLeaseResult> {
        const url = endpoints.global.license.adminLicenseForceUpdate;

        return this.post<LicenseLeaseResult>(url, null, null)
            .done((result: LicenseLeaseResult) => {
                if (result.Status === "Updated") {
                    this.reportSuccess("Your license was successfully updated");
                }
            })
            .fail((response: JQueryXHR) => {
                const message =
                    response.status === 405
                        ? "License activation has been disabled on this server"
                        : "Failed to activate license";

                this.reportError(message, response.responseText, response.statusText);
            });
    }

    static handleNotModifiedStatus(isExpired: boolean) {
        messagePublisher.reportWarning(
            "The license was fetched successfully, " + (isExpired ? "but is still expired" : "but is the same")
        );
    }
}

export = forceLicenseUpdateCommand;
