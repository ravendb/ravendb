import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveCdcSinkCommand extends commandBase {

    private readonly db: database | string;
    private readonly payload: Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration;

    constructor(db: database | string, payload: Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration) {
        super();
        this.payload = payload;
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        return this.save()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to save CDC Sink task`, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Saved CDC Sink task`);
            });
    }

    private save(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        const args = {
            id: this.payload.TaskId || undefined,
        };

        const url = endpoints.databases.ongoingTasks.adminCdcSink + this.urlEncodeArgs(args);

        return this.put(url, JSON.stringify(this.payload), this.db);
    }
}

export = saveCdcSinkCommand;
