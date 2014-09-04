import commandBase = require("commands/commandBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");

class backupDatabaseCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        var url = '/admin/compact' + this.urlEncodeArgs({ database: this.db.name });
        return this.post(url, null, appUrl.getSystemDatabase(), { dataType: 'text' })
            .done(() => this.reportSuccess("Compact completed"))
            .fail((response: JQueryXHR) => {
                if (response.status == 400) {
                    this.reportWarning("Compaction is only supported for Esent.", response.responseText, response.statusText);
                } else {
                    this.reportError("Failed to compact database!", response.responseText, response.statusText);
                }
            });
    }
}

export = backupDatabaseCommand;