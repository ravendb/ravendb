import commandBase = require("commands/commandBase");

class licenseActivateCommand extends commandBase {

    //TODO: use server type!
    constructor(private licensePayload: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        //TODO: use endpoints

        const args = {
            license: this.licensePayload
        };

        return this.post("/license/activate", JSON.stringify(args), null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to activate license", response.responseText, response.statusText);
            });
    }
}

export = licenseActivateCommand; 
