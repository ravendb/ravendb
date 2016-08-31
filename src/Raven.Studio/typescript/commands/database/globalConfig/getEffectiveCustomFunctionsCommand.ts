import commandBase = require("commands/commandBase");
import database = require("models/resources/database");

class getEffectiveCustomFunctionsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<configurationDocumentDto<customFunctionsDto>> {
        var url = "/configuration/document/Raven/Javascript/Functions";//TODO: use endpoints
        return this.query<configurationDocumentDto<customFunctionsDto>>(url, null, this.db);
    }

}

export = getEffectiveCustomFunctionsCommand; 
