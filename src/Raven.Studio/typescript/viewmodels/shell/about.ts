import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import license = require("models/auth/licenseModel");
import registration = require("viewmodels/shell/registration");
import deactivateLicenseCommand = require("commands/licensing/deactivateLicenseCommand");
import buildInfo = require("models/resources/buildInfo");
import messagePublisher = require("common/messagePublisher");

class about extends viewModelBase {

    clientVersion = shell.clientVersion;
    serverVersion = buildInfo.serverBuildVersion;

    spinners = {
        deactivatingLicense: ko.observable<boolean>(false)
    }

    formattedExpiration = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();

        if (!licenseStatus || !licenseStatus.FormattedExpiration) {
            return null;
        }

        return licenseStatus.FormattedExpiration;
    });

    licenseType = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus || licenseStatus.Type === "None") {
            return "No license";
        }

        if (licenseStatus.Type === "Invalid") {
            return "Invalid license";
        }

        return licenseStatus.Type + " License";
    });

    shortDescription = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus || !license.licenseShortDescription()) {
            return null;
        }

        return license.licenseShortDescription();
    });

    registered = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus) {
            return false;
        }

        return licenseStatus.Type !== "None" && licenseStatus.Type !== "Invalid";
    });

    register() {
        registration.showRegistrationDialog(license.licenseStatus(), false, true);
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
	    	    .done(() => {
                        license.fetchLicenseStatus()
                        messagePublisher.reportWarning("Your license was successfully deactivated");
                    })
                    .always(() => this.spinners.deactivatingLicense(false));
            });
    }

    openFeedbackForm() {
        shell.openFeedbackForm();
    }
}

export = about;
