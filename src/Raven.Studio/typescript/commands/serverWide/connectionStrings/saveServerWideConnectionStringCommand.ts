import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import connectionStringsTypes = require("components/pages/database/settings/connectionStrings/store/connectionStringsMapsFromDto");

class saveServerWideConnectionStringCommand extends commandBase {
    constructor(private readonly connectionString: connectionStringsTypes.ServerWideConnectionStringDto) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideConnectionStrings;

        return this.put<void>(url, JSON.stringify(this.connectionString))
            .fail((response: JQueryXHR) =>
                this.reportError(
                    "Failed to save server-wide connection string",
                    response.responseText,
                    response.statusText
                )
            )
            .done(() => this.reportSuccess(`Server-wide connection string was saved successfully`));
    }
}

export = saveServerWideConnectionStringCommand;
