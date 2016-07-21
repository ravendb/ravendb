import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class resolveConflictsCommand extends commandBase {

    constructor(private strategy: number, private fs: filesystem, private reportResolveProgress = true) {
        super();
    }

    execute(): JQueryPromise<any> {

        if (this.reportResolveProgress) {
            this.reportInfo("Resolving conflicts ...");
        }

        var url = "/synchronization/resolveConflicts?strategy=" + this.strategy;
        var resolveTask = this.patch(url, null, this.fs);

        if (this.reportResolveProgress) {
            resolveTask.done(() => this.reportSuccess("Resolved conflicts"));
            resolveTask.fail((response: JQueryXHR) => {
                this.reportError("Failed to resolve conflicts", response.responseText, response.statusText);
            });
        }

        return resolveTask;
    }

} 

export = resolveConflictsCommand;
