import commandBase = require("commands/commandBase");
import database = require("models/database");

class getEffectiveSqlReplicationConnectionStringsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<configurationDocumentDto<sqlReplicationConnectionsDto>> {
        var url = "/configuration/document/Raven/SqlReplication/Connections";
        return this.query<configurationDocumentDto<sqlReplicationConnectionsDto>>(url, null, this.db);
    }

}

export = getEffectiveSqlReplicationConnectionStringsCommand; 