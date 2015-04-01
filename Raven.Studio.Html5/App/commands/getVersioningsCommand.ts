import commandBase = require("commands/commandBase");
import database = require("models/database");
import versioningEntry = require("models/versioningEntry");

class getVersioningsCommand extends commandBase {
    constructor(private db: database, private getGlobalConfig = false) {
        super();
    }

    execute(): JQueryPromise<Array<versioningEntry>> {
        var documentResult = $.Deferred();
        var resultsSelector = (result: versioningEntryDto[]) =>
            result.map((dto: versioningEntryDto) => new versioningEntry(dto, true));
        var url = "/docs";
        var args = {
            startsWith: this.getGlobalConfig ? "Raven/Global/Versioning": "Raven/Versioning",
            start: 0,
            pageSize: 1024
        };

        var postResult = this.query(url, args, this.db, resultsSelector);
        postResult.fail(xhr => documentResult.fail(xhr));
        postResult.done((entries: versioningEntry[]) => documentResult.resolve(entries));
        return documentResult;
    }
}

export = getVersioningsCommand;