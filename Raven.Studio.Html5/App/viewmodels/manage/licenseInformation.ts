import viewModelBase = require("viewmodels/viewModelBase");
import licenseCheckConnectivityCommand = require("commands/auth/licenseCheckConnectivityCommand");
import forceLicenseUpdate = require("commands/auth/forceLicenseUpdate");
import licensingStatus = require("viewmodels/common/licensingStatus");
import app = require("durandal/app");
import license = require("models/auth/license");
import getLicenseStatusCommand = require("commands/auth/getLicenseStatusCommand");
import getSupportCoverageCommand = require("commands/auth/getSupportCoverageCommand");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");

class licenseInformation extends viewModelBase {

    settingsAccess = new settingsAccessAuthorizer();
    connectivityStatus = ko.observable<string>("pending");
    isForbidden = ko.observable<boolean>();

    attached() {
        super.attached();

        if (!this.settingsAccess.canReadOrWrite()) {
            this.checkConnectivity()
                .done((result) => {
                    this.connectivityStatus(result ? "success" : "failed");
                })
                .fail(() => this.connectivityStatus("failed"));
        }
    }

    fetchLicenseStatus() {
        return new getLicenseStatusCommand()
            .execute()
            .done((result: licenseStatusDto) => {
            if (result.Status.contains("AGPL")) {
                result.Status = "Development Only";
            }
            license.licenseStatus(result);
        });
    }

    fetchSupportCoverage() {
        return new getSupportCoverageCommand()
            .execute()
            .done((result: supportCoverageDto) => {
                license.supportCoverage(result);
            });
    }


    forceUpdate() {
        new forceLicenseUpdate().execute()
            .always(() => {
                $.when(this.fetchLicenseStatus(), this.fetchSupportCoverage())
                    .always(() => {
                        this.showLicenseDialog();
                    });
            });
    }

    private showLicenseDialog() {
        var dialog = new licensingStatus(license.licenseStatus(), license.supportCoverage(), license.hotSpare());
        app.showDialog(dialog);
    }

    checkConnectivity(): JQueryPromise<boolean> {
        return new licenseCheckConnectivityCommand().execute();
    }
}

export =licenseInformation;
