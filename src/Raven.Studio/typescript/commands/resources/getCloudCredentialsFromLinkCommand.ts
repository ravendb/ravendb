import commandBase = require("commands/commandBase");

class getCloudCredentialsFromLinkCommand extends commandBase {

    constructor(private link: string) {
        super();
    }

    execute(): JQueryPromise<string> {
        return this.query<string>(this.link, undefined)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get restore credentials`,
                    response.responseText,
                    response.statusText);
            });
    }
}

export = getCloudCredentialsFromLinkCommand; 
