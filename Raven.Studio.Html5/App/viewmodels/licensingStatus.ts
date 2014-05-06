import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class licensingStatus extends dialogViewModelBase {

    bundles = ["SQL Replication", "Scripted Index"];
    bundleMap = { compression: "Compression", encryption: "Encryption:", documentExpiration: "Expiration", quotas: "Quotas", replication: "Replication", versioning: "Versioning", periodicBackup: "Periodic Backup"};
    bundleString = "";

    constructor(private licenseStatus: licenseStatusDto) {
        super();
        for (var key in licenseStatus.Attributes) {
            var name = this.bundleMap[key];
            var isBundle = (name !== undefined);
            if (licenseStatus.Attributes.hasOwnProperty(key) && isBundle) {
                this.bundles.push(name);
            }
        }
        this.bundleString = this.bundles.sort().join(", ");
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
