import commandBase = require("commands/commandBase");

class licenseRegistrationCommand extends commandBase {

    //TODO: use server type!
    constructor(private registrationData: any) {
        super();
    }

    execute(): JQueryPromise<void> {
        //TODO: use endpoints

        return this.post("/license/registration", JSON.stringify(this.registrationData), null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to send registration information", response.responseText, response.statusText);
            });
    }
}

export = licenseRegistrationCommand; 
