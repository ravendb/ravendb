import commandBase = require("commands/commandBase");
import database = require("models/database");

class getReplicationsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<configurationDocumentDto<replicationsDto>> {
        var url = "/configuration/document/Raven/Replication/Destinations";
        return this.query<configurationDocumentDto<replicationsDto>>(url, null, this.db);
    }

}

export = getReplicationsCommand; 