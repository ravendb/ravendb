import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class saveDestinationCommand extends commandBase {

    constructor(private dto: synchronizationReplicationsDto, private fs: filesystem) {
        super();
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Replication destinations.");
        return this.saveSetup()
            .done(() => this.reportSuccess("Saved Replication destinations."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Replication destinations.", response.responseText, response.statusText));
    }

    private saveSetup(): JQueryPromise<any> {
        var name = "Raven/Synchronization/Destinations";
        var url = "/config?name=" + encodeURIComponent(name);
        var putArgs = JSON.stringify(this.dto);
        return this.put(url, putArgs, this.fs);
    }
}

export = saveDestinationCommand;