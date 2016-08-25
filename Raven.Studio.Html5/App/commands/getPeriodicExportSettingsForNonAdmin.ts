import commandBase = require("commands/commandBase");
import database = require("models/database");

class getPeriodicExportSettingsForNonAdmin extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<any> {
        return this.query("/periodicExport/settings", null, this.db);
    }

}
export = getPeriodicExportSettingsForNonAdmin;
