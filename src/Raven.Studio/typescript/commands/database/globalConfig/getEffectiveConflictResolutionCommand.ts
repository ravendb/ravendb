import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getEffectiveConflictResolutionCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<configurationDocumentDto<replicationConfigDto>> {
        var url = "/configuration/document/Raven/Replication/Config";//TODO: use endpoints
        return this.query<configurationDocumentDto<replicationConfigDto>>(url, null, this.db);
    }

}

export = getEffectiveConflictResolutionCommand; 
