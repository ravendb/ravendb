import commandBase = require("commands/commandBase");
import database = require("models/database");

class getDocementsMetadataByIDPrefixCommand extends commandBase {

    constructor(private prefix:string,private db: database) {
        super();
    }

    
}

export = getDocementsMetadataByIDPrefixCommand;