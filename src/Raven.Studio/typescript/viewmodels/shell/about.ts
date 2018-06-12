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
    supportTableCssClass = license.supportTableCssClass;
    
    clientVersion = shell.clientVersion;
    serverVersion = buildInfo.serverBuildVersion;

    developerLicense = license.developerLicense;
    
    maxClusterSize  = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return licenseStatus ? licenseStatus.MaxClusterSize : 1;
    });
    
    maxCores = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return licenseStatus ? licenseStatus.MaxCores : 3;
    });
    
    maxMemory = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return licenseStatus ? licenseStatus.MaxMemory : 6;
    });
    
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
        const nextMonth = moment().add(1, 'month');
        if (now.isBefore(expiration)) {
            const relativeDurationClass = nextMonth.isBefore(expiration) ? "" : "text-warning";
            
            const fromDuration = generalUtils.formatDurationByDate(expiration, false);
            return `${expiration.format(dateFormat)} <br /><small class="${relativeDurationClass}">(in ${fromDuration})</small>`;
        }

        const duration = generalUtils.formatDurationByDate(expiration, true);
        return `${expiration.format(dateFormat)} <br /><Small class="text-danger">(${duration} ago)</Small>`;
    });

    licenseType = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus || licenseStatus.Type === "None") {
            return "No license - AGPLv3 Restrictions Applied";
        }

        if (licenseStatus.Type === "Invalid") {
            return "Invalid license";
        }

        return licenseStatus.Type;
    });

    hasLicense = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        return licenseStatus && licenseStatus.Type !== "None";
    });

    shortDescription = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        const shortDescription = license.licenseShortDescription();
        if (!licenseStatus || !shortDescription) {
            return null;
        }

        return shortDescription;
    });

    licenseId = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        const licenseId = license.licenseId();
        if (!licenseStatus || !licenseId) {
            return null;
        }

        return licenseId;
    });
    
    registered = ko.pureComputed(() => {
        const licenseStatus = license.licenseStatus();
        if (!licenseStatus) {
            return false;
        }

        return licenseStatus.Type !== "None" && licenseStatus.Type !== "Invalid";
    });
    
    licenseAttribute(name: keyof Raven.Server.Commercial.LicenseStatus) {
        return ko.pureComputed(() => {
           const licenseStatus = license.licenseStatus();
           if (licenseStatus) {
               return licenseStatus[name] ? "icon-checkmark" : "icon-cancel";
           }
           return "icon-cancel";
        });
    }

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
