import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class resolveConflictCommand extends commandBase {

    constructor(private fileName: string, private strategy: number, private fs: filesystem, private reportResolveProgress = true) {
        super();
    }

    execute(): JQueryPromise<any> {

        if (this.reportResolveProgress) {
            this.reportInfo("Resolving conflicts for file " + this.fileName + "...");
        }

        var url = "/synchronization/resolveConflict/" + encodeURIComponent(this.fileName) + "?strategy=" + this.strategy;
        var resolveTask = this.patch(url, null, this.fs);

        if (this.reportResolveProgress) {
            resolveTask.done(() => this.reportSuccess("Resolved conflicts for file " + this.fileName));
            resolveTask.fail((response: JQueryXHR) => {
                this.reportError("Failed to resolve conflicts for file " + this.fileName, response.responseText, response.statusText);
            });
        }

        return resolveTask;
    }

} 

export = resolveConflictCommand;
