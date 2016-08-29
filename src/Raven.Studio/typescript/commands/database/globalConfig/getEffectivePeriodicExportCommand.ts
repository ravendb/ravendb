import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getEffectivePriodicExportCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<configurationDocumentDto<periodicExportSetupDto>> {
        var url = "/configuration/document/Raven/Backup/Periodic/Setup";//TODO: use endpoints
        return this.query<configurationDocumentDto<periodicExportSetupDto>>(url, null, this.db);
    }

}

export = getEffectivePriodicExportCommand; 
