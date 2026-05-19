import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDatabaseFooterStatsCommand extends commandBase {

    constructor(private db: database | string, private nodeTag?: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Studio.FooterStatistics> {
        const url = endpoints.databases.studioStats.studioFooterStats;
        const args = this.nodeTag ? { nodeTag: this.nodeTag } : null;
        return this.query<Raven.Server.Documents.Studio.FooterStatistics>(url, args, this.db);
    }
}

export = getDatabaseFooterStatsCommand;
