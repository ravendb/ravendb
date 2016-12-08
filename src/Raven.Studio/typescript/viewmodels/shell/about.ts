import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import license = require("models/auth/license");
import registration = require("viewmodels/shell/registration");
import moment = require("moment");

class about extends viewModelBase {

    clientVersion = shell.clientVersion;
    serverVersion = shell.serverBuildVersion;
    licenseStatus = license.licenseStatus;

    expiration = ko.pureComputed(() => {
        const licenseStatus = this.licenseStatus();

        if (!licenseStatus) {
            return null;
        }

        if (licenseStatus.LicenseType === "Commercial" || licenseStatus.LicenseType === "PreRelease" || licenseStatus.LicenseType === "Dev") {
            const expiration = licenseStatus.Attributes["expiration"];

            if (expiration) {
                return moment(expiration).format("LL");
            }
        }

        return null;
    });

    licenseType = ko.pureComputed(() => {
        const licenseStatus = this.licenseStatus();
        if (!licenseStatus || licenseStatus.LicenseType === "None") {
            return "No license";
        }

        if (licenseStatus.LicenseType === "Invalid") {
            return "Invalid license";
        }

        return licenseStatus.LicenseType + " License";

    });

    registered = ko.pureComputed(() => {
        const licenseStatus = this.licenseStatus();
        if (!licenseStatus) {
            return false;
        }

        return licenseStatus.LicenseType === "PreRelease" ||
            licenseStatus.LicenseType === "Commercial" ||
            licenseStatus.LicenseType === "Dev";
    });

    register() {
        registration.showRegistrationDialog(this.licenseStatus(), true);
    }

}

export = about;