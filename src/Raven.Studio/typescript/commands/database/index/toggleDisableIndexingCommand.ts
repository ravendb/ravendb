import commandBase = require("commands/commandBase");
import databaseInfo = require("models/resources/info/databaseInfo");
import endpoints = require("endpoints");
import configuration = require("configuration");

class toggleDisableIndexingCommand extends commandBase {

    constructor(private start: boolean, private db: databaseInfo) {
        super();
    }

    private updateDocument(basicUrl: string, databaseConfigDocument: documentDto): JQueryPromise<void> {
        this.start ? delete databaseConfigDocument["Settings"][configuration.indexing.disabled] :
            databaseConfigDocument["Settings"][configuration.indexing.disabled] = true;

        const jQueryOptions: JQueryAjaxSettings = {
            headers: { "ETag": databaseConfigDocument["Etag"] }
        };

        return this.put<void>(basicUrl, JSON.stringify(databaseConfigDocument), null, jQueryOptions)
            .done(() => {
                const state = this.start ? "Enabled" : "Disabled";
                this.reportSuccess(`Indexing is ${state}`);
            }).fail((response: JQueryXHR) => this.reportError("Failed to toggle indexing status", response.responseText));
    }

    execute(): JQueryPromise<void> {
        const args = {
            "name": this.db.name
        };
        const basicUrl = endpoints.global.adminDatabases.adminDatabases + this.urlEncodeArgs(args);

        return this.query(basicUrl, null).then((databaseConfigDocument: documentDto) => 
            this.updateDocument(basicUrl, databaseConfigDocument));
    }
}

export = toggleDisableIndexingCommand;