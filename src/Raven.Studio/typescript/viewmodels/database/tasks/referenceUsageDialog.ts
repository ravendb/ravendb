import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import rootSqlTable = require("models/database/tasks/sql/rootSqlTable");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class referenceUsageDialog extends dialogViewModelBase {
    
    constructor(private table: rootSqlTable, private references: Array<sqlReference>, private action: (ref: sqlReference, action: sqlMigrationAction) => void) {
        super();
        
        this.bindToCurrentInstance("onActionClicked");
    }
    
    onActionClicked(reference: sqlReference, action: sqlMigrationAction) {
        this.action(reference, action);
    }
}

export = referenceUsageDialog; 
