import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteServerWideConnectionStringCommand extends commandBase {
    constructor(
        private readonly type: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType,
        private readonly connectionStringName: string
    ) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            type: this.type,
            name: this.connectionStringName,
        };

        const url =
            endpoints.global.adminServerWide.adminConfigurationServerWideConnectionStrings +
            this.urlEncodeArgs(args);

        return this.del<void>(url, null)
            .done(() =>
                this.reportSuccess(`Successfully deleted server-wide connection string - ${this.connectionStringName}`)
            )
            .fail((response: JQueryXHR) =>
                this.reportError(
                    `Failed to delete server-wide connection string - ${this.connectionStringName}`,
                    response.responseText,
                    response.statusText
                )
            );
    }
}

export = deleteServerWideConnectionStringCommand;
