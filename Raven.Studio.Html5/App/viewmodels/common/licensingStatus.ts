import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class licensingStatus extends dialogViewModelBase {

    bundles = ["SQL Replication", "Scripted Index"];
    bundleMap = { compression: "Compression", encryption: "Encryption:", documentExpiration: "Expiration", quotas: "Quotas", replication: "Replication", versioning: "Versioning", periodicBackup: "Periodic Export"};
    bundleString = "";

    supportClass = ko.observable<string>();
    supportText = ko.observable<string>();

    constructor(private licenseStatus: licenseStatusDto, supportCoverage: supportCoverageDto) {
        super();
        for (var key in licenseStatus.Attributes) {
            var name = this.bundleMap[key];
            var isBundle = (name !== undefined);
            if (licenseStatus.Attributes.hasOwnProperty(key) && licenseStatus.Attributes[key] === "true" && isBundle) {
                this.bundles.push(name);
            }
        }
        this.bundleString = this.bundles.sort().join(", ");

        switch (supportCoverage.Status) {
            case "NoSupport":
                this.supportClass('text-warning');
                break;
            case "LicenseNotFound":
            case "InvalidStateSupportNotFound":
                this.supportClass('license-error');
                break;
            case "PartialSupport":
            case "ProfessionalSupport":
            case "ProductionSupport":
                this.supportClass('text-success');
                break;
        }

        this.supportText(supportCoverage.Status);
    }

    cancel() {
        dialog.close(this);
    }

    ok() {
        dialog.close(this);
    }

    private isBundle(bundle) {
        return this.bundleMap[bundle] !== undefined;
    }

}

export = licensingStatus;
