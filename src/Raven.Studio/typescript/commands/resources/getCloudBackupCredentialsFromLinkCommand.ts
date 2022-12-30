import commandBase = require("commands/commandBase");

class getCloudBackupCredentialsFromLinkCommand extends commandBase {

    private link: string;

    constructor(link: string) {
        super();
        this.link = link;
    }

    execute(): JQueryPromise<federatedCredentials> {
        return this.query<federatedCredentials>(this.link, undefined);
    }
}

export = getCloudBackupCredentialsFromLinkCommand;
