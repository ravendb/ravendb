import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import connectionStringsMapsFromDto = require("components/pages/database/settings/connectionStrings/store/connectionStringsMapsFromDto");

interface GetServerWideConnectionStringsResult {
    Results: connectionStringsMapsFromDto.ServerWideConnectionStringDto[];
}

class getServerWideConnectionStringsCommand extends commandBase {
    execute(): JQueryPromise<GetServerWideConnectionStringsResult> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideConnectionStrings;

        return this.query<GetServerWideConnectionStringsResult>(url, null, null)
            .fail((response: JQueryXHR) => {
                this.reportError(
                    "Failed to get server-wide connection strings",
                    response.responseText,
                    response.statusText
                );
            });
    }
}

export = getServerWideConnectionStringsCommand;
