import commandBase from "commands/commandBase";
import database from "models/resources/database";
import endpoints from "endpoints";

export default class openFaultyIndexCommand extends commandBase {

    constructor(private indexNameToOpen: string, private db: database, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexNameToOpen,
            ...this.location
        };

        const url = endpoints.databases.index.indexOpenFaultyIndex + this.urlEncodeArgs(args);
        return this.post(url, null, this.db)
            .done(() => this.reportSuccess(`Faulty index ${this.indexNameToOpen} was successfully opened`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to open faulty index: ${this.indexNameToOpen}`, response.responseText, response.statusText));
    }
}
