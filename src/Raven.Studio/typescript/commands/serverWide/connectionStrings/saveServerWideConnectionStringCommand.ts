import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveServerWideConnectionStringCommand extends commandBase {
    constructor(private readonly connectionString: any) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideConnectionStrings;

        const payload = {
            ExcludedDatabases: [],
            ...this.connectionString,
        };

        return this.put<void>(url, JSON.stringify(payload))
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
