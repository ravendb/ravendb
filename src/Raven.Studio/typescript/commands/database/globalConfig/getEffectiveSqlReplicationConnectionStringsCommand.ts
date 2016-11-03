import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getEffectiveSqlReplicationConnectionStringsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }
    /* TODO
    execute(): JQueryPromise<configurationDocumentDto<sqlReplicationConnectionsDto>> {
        var url = "/configuration/document/Raven/SqlReplication/Connections";//TODO: use endpoints
        return this.query<configurationDocumentDto<sqlReplicationConnectionsDto>>(url, null, this.db);
    }*/

}

export = getEffectiveSqlReplicationConnectionStringsCommand; 
