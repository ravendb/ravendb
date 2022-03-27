import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getSuggestedConflictResolutionCommand extends commandBase {

    constructor(private ownerDb: database, private documentId: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Utils.ConflictResolverAdvisor.MergeResult> {
        const args = {
            docId: this.documentId
        };
        const url = endpoints.databases.studioDatabaseTasks.studioTasksSuggestConflictResolution + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Utils.ConflictResolverAdvisor.MergeResult>(url, null, this.ownerDb);
    }
}

export = getSuggestedConflictResolutionCommand;
