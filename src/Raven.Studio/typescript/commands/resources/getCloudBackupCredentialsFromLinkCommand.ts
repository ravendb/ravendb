import commandBase = require("commands/commandBase");

class getCloudBackupCredentialsFromLinkCommand extends commandBase {

    constructor(private link: string) {
        super();
    }

    execute(): JQueryPromise<federatedCredentials> {
        return this.query<federatedCredentials>(this.link, undefined)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get backup credentials`, response.responseText, response.statusText);
            });
    }
}

export = getCloudBackupCredentialsFromLinkCommand;
