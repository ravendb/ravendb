import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import license = require("models/auth/license");
import registration = require("viewmodels/shell/registration");
import deactivateLicenseCommand = require("commands/licensing/deactivateLicenseCommand");

class about extends viewModelBase {

    clientVersion = shell.clientVersion;
    serverVersion = shell.serverBuildVersion;
    licenseStatus = license.licenseStatus;

    spinners = {
        deactivatingLicense: ko.observable<boolean>(false)
    }

    formattedExpiration = ko.pureComputed(() => {
        const licenseStatus = this.licenseStatus();

        if (!licenseStatus || !licenseStatus.FormattedExpiration) {
            return null;
        }

        return licenseStatus.FormattedExpiration;
    });

    licenseType = ko.pureComputed(() => {
        const licenseStatus = this.licenseStatus();
        if (!licenseStatus || licenseStatus.Type === "None") {
            return "No license";
        }

        if (licenseStatus.Type === "Invalid") {
            return "Invalid license";
        }

        let licenseType = licenseStatus.Type;
        if (licenseType === "Free") {
            licenseType += " Single Node";
        }

        return licenseType + " License";
    });

    shortDescription = ko.pureComputed(() => {
        const licenseStatus = this.licenseStatus();
        if (!licenseStatus || !licenseStatus.ShortDescription) {
            return null;
        }

        return licenseStatus.ShortDescription;
    });

    registered = ko.pureComputed(() => {
        const licenseStatus = this.licenseStatus();
        if (!licenseStatus) {
            return false;
        }

        return licenseStatus.Type !== "None" && licenseStatus.Type !== "Invalid";
    });

    register() {
        registration.showRegistrationDialog(this.licenseStatus(), true);
    }

    deactivateLicense() {
        this.confirmationMessage(
                "Deactivate",
                "Are you sure that you want to deactivate this license?")
            .done(can => {
                if (!can) {
                    return;
                }

                this.spinners.deactivatingLicense(true);
                new deactivateLicenseCommand().execute()
                    .done(() => this.licenseStatus(null))
                    .always(() => this.spinners.deactivatingLicense(false));
            });
    }

    openFeedbackForm() {
        shell.openFeedbackForm();
    }
}

export = about;