import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export default class saveCdcSinkTaskCommand extends commandBase {
    constructor(
        private db: string,
        private payload: Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        const args = {
            id: this.payload.TaskId || undefined,
        };

        const url = endpoints.databases.ongoingTasks.adminCdcSink + this.urlEncodeArgs(args);

        return this.put<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult>(
            url,
            JSON.stringify(this.payload),
            this.db
        )
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save CDC Sink task", response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`CDC Sink task was saved successfully`);
            });
    }
}
