import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getDebugPackageDatabaseSettingsCommand extends commandBase {
    constructor(
        private packageId: string,
        private nodeTag: string,
        private databaseName: string
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Config.SettingsResult> {
        const url =
            endpoints.global.debugPackageAnalyzer.debugInfoPackageAnalyzerDatabasesConfigurationSettings +
            this.urlEncodeArgs({ packageId: this.packageId, nodeTag: this.nodeTag, name: this.databaseName });

        return this.query<Raven.Server.Config.SettingsResult>(url, null, null);
    }
}

export = getDebugPackageDatabaseSettingsCommand;
