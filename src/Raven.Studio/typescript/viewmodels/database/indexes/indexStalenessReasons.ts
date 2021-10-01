import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import getIndexStalenessReasonsCommand = require("commands/database/index/getIndexStalenessReasonsCommand");

class indexStalenessReasons extends dialogViewModelBase {

    view = require("views/database/indexes/indexStalenessReasons.html");
    
    private db: database;
    indexName: string;
    reasons = ko.observable<indexStalenessReasonsResponse>();
    
    constructor(db: database, indexName: string) {
        super();
        this.db = db;
        this.indexName = indexName;
    }
    
    activate() {
        return new getIndexStalenessReasonsCommand(this.indexName, this.db)
            .execute()
            .done(reasons => {
                this.reasons(reasons);
            });
    }

}

export = indexStalenessReasons;
