import viewModelBase = require("viewmodels/viewModelBase");
import licenseCheckConnectivityCommand = require("commands/auth/licenseCheckConnectivityCommand");
import forceLicenseUpdate = require("commands/auth/forceLicenseUpdate");
import licensingStatus = require("viewmodels/common/licensingStatus");
import app = require("durandal/app");
import license = require("models/auth/license");
import getLicenseStatusCommand = require("commands/auth/getLicenseStatusCommand");

class licenseInformation extends viewModelBase {

	connectivityStatus = ko.observable<string>("pending");

	attached() {
		super.attached();
		this.checkConnectivity()
			.done((result) => {
				this.connectivityStatus(result ? "success" : "failed");
			})
			.fail(() => this.connectivityStatus("failed"));
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

	forceUpdate() {
		new forceLicenseUpdate().execute()
			.always(() => {
				this.fetchLicenseStatus()
					.always(() => this.showLicenseDialog());
			});
	}

	private showLicenseDialog() {
        var dialog = new licensingStatus(license.licenseStatus());
        app.showDialog(dialog);
	}

	checkConnectivity(): JQueryPromise<boolean> {
		return new licenseCheckConnectivityCommand().execute();
	}
}

export =licenseInformation;