import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import rootSqlTable = require("models/database/tasks/sql/rootSqlTable");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class referenceUsageDialog extends dialogViewModelBase {

    view = require("views/database/tasks/referenceUsageDialog.html");
    
    constructor(private table: rootSqlTable, private references: Array<sqlReference>, private action: (ref: sqlReference, action: sqlMigrationAction) => void,
                private goToTableFunc: (targetTable: rootSqlTable) => void) {
        super();
        
        this.bindToCurrentInstance("onActionClicked", "goToTable");
    }
    
    onActionClicked(reference: sqlReference, action: sqlMigrationAction) {
        this.action(reference, action);
    }
    
    goToTable(targetTable: rootSqlTable) {
        this.close();
        this.goToTableFunc(targetTable);
    }
    
}

export = referenceUsageDialog; 
