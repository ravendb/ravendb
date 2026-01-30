import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import moment = require("moment");

class licenseSendVerificationCodeCommand extends commandBase {
    constructor(private licensePayload: SendFreeLicenseVerificationRequest) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.license.licenseFreeSendVerificationCode;
        
        return this.post(url, JSON.stringify(this.licensePayload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                if (response.status === 429) {
                    this.reportError(`Too many requests, please try again in few minutes. (${moment.duration(response.getResponseHeader("retry-after"), "seconds").asMinutes()} minutes)`, response.responseText, response.statusText);
                    return;
                }
                
                this.reportError("Unable to generate license.", response.responseText, response.statusText);
            });
    }
}

export = licenseSendVerificationCodeCommand