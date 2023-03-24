import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import pluralizeHelpers = require("common/helpers/text/pluralizeHelpers");

class forceTombstonesCleanup extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<number> {
        const url = endpoints.databases.adminTombstone.adminTombstonesCleanup + this.urlEncodeArgs(this.location)
        return this.post(url, null, this.db)
            .done((result: { Value: number }) => {
                if (result.Value === 0) {
                    this.reportSuccess("No tombstones to cleanup");
                } else {
                    const amount = pluralizeHelpers.pluralize(result.Value, "tombstone", "tombstones");
                    this.reportSuccess("Successfully cleaned up " + amount);    
                }
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to cleanup tombstones", response.responseText, response.statusText));
    }
}

export = forceTombstonesCleanup;
