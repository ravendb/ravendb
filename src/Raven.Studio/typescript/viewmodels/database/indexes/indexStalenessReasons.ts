import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import getIndexStalenessReasonsCommand = require("commands/database/index/getIndexStalenessReasonsCommand");
import genUtils from "common/generalUtils";

class indexStalenessReasons extends dialogViewModelBase {

    view = require("views/database/indexes/indexStalenessReasons.html");
    
    private db: database;
    indexName: string;
    reasons = ko.observable<indexStalenessReasonsResponse>();
    location: databaseLocationSpecifier;
    
    constructor(db: database, indexName: string, location?: databaseLocationSpecifier) {
        super();
        this.db = db;
        this.indexName = indexName;
        this.location = location;
    }
    
    activate() {
        return new getIndexStalenessReasonsCommand(this.indexName, this.db, this.location)
            .execute()
            .done(reasons => {
                this.reasons(reasons);
            });
    }
    
    formatLocation() {
        return genUtils.formatLocation(this.location);
    }
}

export = indexStalenessReasons;
