import filesystem = require("models/filesystem/filesystem");

import versioningEntry = require("models/filesystem/versioningEntry");

import commandBase = require("commands/commandBase");
import saveVersioningCommand = require("commands/filesystem/saveVersioningCommand");

class createDefaultSettingsCommand extends commandBase {
    constructor(private fs: filesystem, private bundles) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Creating default settings for '" + this.fs.name + "'...");
        
        var tasksToWatch = []; 
        if (this.bundles.contains("Versioning")) {
            tasksToWatch.push(this.saveVersioningConfiguration());
        }

        if (tasksToWatch.length > 0) {
            return $.when.apply(null, tasksToWatch);
        } else {
            return $.Deferred().resolve();
        }
    }

    private createDefaultVersioning(): versioningEntry {
        return new versioningEntry({
            Id: "Raven/Versioning/DefaultConfiguration",
            MaxRevisions: 5,
            Exclude: false,
            ExcludeUnlessExplicit: false,
            PurgeOnDelete: false
        });
    }

    private saveVersioningConfiguration(): JQueryPromise<any> {
        return new saveVersioningCommand(this.fs, this.createDefaultVersioning().toDto()).execute();
    }
}

export = createDefaultSettingsCommand;