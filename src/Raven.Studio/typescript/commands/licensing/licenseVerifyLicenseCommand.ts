import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class licenseVerifyLicenseCommand extends commandBase {
    constructor(private verifyLicensePayload: DownloadFreeLicenseRequest) {
        super();
    }
    
    execute(): JQueryPromise<DownloadFreeLicenseResponse> {
        const url = endpoints.global.license.licenseFreeDownload;
        
        const task = $.Deferred<DownloadFreeLicenseResponse>();
        
        return this.post<DownloadFreeLicenseResponse>(url, JSON.stringify(this.verifyLicensePayload), null, { dataType: undefined })
            .done((result: DownloadFreeLicenseResponse) => {
                if (result.LicenseDownloadStatus !== "Success") {
                    task.reject(result.LicenseDownloadStatus);
                    this.reportError("License verification failed", result.LicenseDownloadStatus);
                    return;
                }
                task.resolve(result);
            })
            .fail((response: JQueryXHR) => {
                this.reportError(response.responseText, response.responseText, response.statusText);
                task.reject(response);
            });
    }
}

export = licenseVerifyLicenseCommand;