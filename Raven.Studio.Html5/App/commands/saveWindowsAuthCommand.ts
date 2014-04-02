import commandBase = require("commands/commandBase");
import getLicenseStatusCommand = require("commands/getLicenseStatusCommand");
import document = require("models/document");
import database = require("models/database");

class saveWindowsAuthCommand extends commandBase {

    constructor(private dto: windowsAuthDto) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.fetchLicenseStatus();
    }

    private fetchLicenseStatus(): JQueryPromise<any> {
        return new getLicenseStatusCommand()
            .execute()
            .done((result: licenseStatusDto) => this.handleLicenseStatus(result));
    }

    private handleLicenseStatus(licenseStatus: licenseStatusDto) {
        //if (licenseStatus.IsCommercial || licenseStatus.ValidCommercialLicenseSeen) {
            this.performWithCommercialLicense();
        //} else {
            //this.reportWarning("Cannot setup Windows Authentication without a valid commercial license.");
        //}
    }

    private performWithCommercialLicense() {
        if (this.dto.RequiredUsers.concat(this.dto.RequiredGroups).every(element => (element.Name.indexOf("\\") !== -1))) {
            this.performSave();
        } else {
            this.reportWarning("Windows Authentication not saved! All names must have \"\\\" in them.");
        }
    }

    private performSave() {
        this.reportInfo("Saving Windows Authentication settings.");
        this.saveSetup()
            .done(() => this.reportSuccess("Saved Windows Authentication settings."))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Windows Authentication settings.", response.responseText, response.statusText));
    }

    private saveSetup(): JQueryPromise<any> {
        var id = "Raven/Authorization/WindowsSettings";
        var url = "/docs/" + id;
        var putArgs = JSON.stringify(this.dto);
        return this.put(url, putArgs, null);
    }
}

export = saveWindowsAuthCommand;