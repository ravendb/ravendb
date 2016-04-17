import viewModelBase = require("viewmodels/viewModelBase");
import licenseCheckConnectivityCommand = require("commands/auth/licenseCheckConnectivityCommand");
import forceLicenseUpdate = require("commands/auth/forceLicenseUpdate");
import licensingStatus = require("viewmodels/common/licensingStatus");
import app = require("durandal/app");
import license = require("models/auth/license");
import getLicenseStatusCommand = require("commands/auth/getLicenseStatusCommand");
import shell = require("viewmodels/shell");
import getSupportCoverageCommand = require("commands/auth/getSupportCoverageCommand");

class licenseInformation extends viewModelBase {

    connectivityStatus = ko.observable<string>("pending");
    isForbidden = ko.observable<boolean>();

    constructor() {
        super();
        this.isForbidden(shell.isGlobalAdmin() === false);
    }

    attached() {
        super.attached();

        if (this.isForbidden() === false) {
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
        var dialog = new licensingStatus(license.licenseStatus(), license.supportCoverage());
        app.showDialog(dialog);
    }

    checkConnectivity(): JQueryPromise<boolean> {
        return new licenseCheckConnectivityCommand().execute();
    }
}

export =licenseInformation;
