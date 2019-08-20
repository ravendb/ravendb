import commandBase = require("commands/commandBase");

class getCloudBackupCredentialsFromLinkCommand extends commandBase {

    constructor(private link: string) {
        super();
    }

    execute(): JQueryPromise<backupCredentials> {
        return this.query<backupCredentials>(this.link, undefined)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get backup credentials`,
                    response.responseText,
                    response.statusText);
            });
    }
}

export = getCloudBackupCredentialsFromLinkCommand;
