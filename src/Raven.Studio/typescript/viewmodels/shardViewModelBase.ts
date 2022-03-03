import viewModelBase = require("viewmodels/viewModelBase");
import database from "models/resources/database";
import shard from "models/resources/shard";



abstract class shardViewModelBase extends viewModelBase {
    
    protected readonly db: database;
    
    protected constructor(db: database) {
        super();
        
        this.db = db;
    }
    
    /**
     * @deprecated Use database injected via constructor
     * @protected
     */
    protected get activeDatabase(): KnockoutObservable<database> {
        throw new Error("Can't access activate database in single shard view!");
    }

    /**
     * Allows to sot
     */
    getViewState(): any {
        return null;
    }
    
}

export = shardViewModelBase;
