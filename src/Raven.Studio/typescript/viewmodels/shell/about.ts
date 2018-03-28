import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import license = require("models/auth/licenseModel");
import registration = require("viewmodels/shell/registration");
import forceLicenseUpdateCommand = require("commands/licensing/forceLicenseUpdateCommand");
import buildInfo = require("models/resources/buildInfo");
import generalUtils = require("common/generalUtils");
import accessManager = require("common/shell/accessManager");

class about extends viewModelBase {

    accessManager = accessManager.default.aboutView;
    licenseCssClass = license.licenseCssClass;
    supportCssClass = license.supportCssClass;
    
    supportLabel = license.supportLabel;
    
    clientVersion = shell.clientVersion;
    serverVersion = buildInfo.serverBuildVersion;
    
    canUpgradeSupport = ko.pureComputed(() => {
        const support = license.supportCoverage();
        if (!support || !support.Status) {
            return true;
        }
        
        return support.Status !== "ProductionSupport";
    });
    

    spinners = {
        forceLicenseUpdate: ko.observable<boolean>(false)
    };

    formattedExpiration = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();

        if (!licenseStatus || !licenseStatus.Expiration) {
            return null;
        }

        const dateFormat = "YYYY MMMM Do";
        const expiration = moment(licenseStatus.Expiration);
        const now = moment();
        if (now.isBefore(expiration)) {
            const fromDuration = generalUtils.formatDurationByDate(expiration, false);
            return `in ${fromDuration} (${expiration.format(dateFormat)})`;
        }

        const duration = generalUtils.formatDurationByDate(expiration, true);
        return `${duration} ago (${expiration.format(dateFormat)})`;
    });

    licenseType = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus || licenseStatus.Type === "None") {
            return "No license";
        }

        if (licenseStatus.Type === "Invalid") {
            return "Invalid license";
        }

        return licenseStatus.Type;
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

    forceLicenseUpdate() {
        this.confirmationMessage(
                "Force License Update",
                "Are you sure that you want to force license update?")
            .done(can => {
                if (!can) {
                    return;
                }

                this.spinners.forceLicenseUpdate(true);
                new forceLicenseUpdateCommand().execute()
                    .done(() => {
                        license.fetchLicenseStatus()
                            .done(() => license.fetchSupportCoverage());
                    })
                    .always(() => this.spinners.forceLicenseUpdate(false));
            });
    }

    openFeedbackForm() {
        shell.openFeedbackForm();
    }
}

export = about;
