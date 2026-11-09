import commandBase = require("commands/commandBase");

class resumeBackupSchedulerCommand extends commandBase {
    private readonly nodeTag: string;

    constructor(nodeTag: string) {
        super();
        this.nodeTag = nodeTag;
    }

    execute(): JQueryPromise<void> {
        const url = "/admin/backups/scheduler/resume" + this.urlEncodeArgs({ nodeTag: this.nodeTag });

        return this.post<void>(url, null, undefined, { dataType: undefined }).fail((response: JQueryXHR) =>
            this.reportError("Failed to resume backup scheduler", response.responseText, response.statusText)
        );
    }
}

export = resumeBackupSchedulerCommand;
