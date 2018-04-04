import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import rootSqlTable = require("models/database/tasks/sql/rootSqlTable");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class referenceInUseDialog extends dialogViewModelBase {
    
    allResolved: KnockoutComputed<boolean>;
    
    constructor(private table: rootSqlTable, private references: Array<sqlReference>, private action: (ref: sqlReference, action: sqlMigrationAction) => void) {
        super();
        
        this.bindToCurrentInstance("onActionClicked");
        
        this.allResolved = ko.pureComputed(() => {
            let result = true;
            
            this.references.forEach(ref => {
                if (ref.action() === "link") {
                    result = false;
                }
            });
            
            return result;
        })
    }
    
    unselect() {
        this.table.checked(false);
        
        this.close();
    }
    
    onActionClicked(reference: sqlReference, action: sqlMigrationAction) {
        this.action(reference, action);
    }
}

export = referenceInUseDialog; 
