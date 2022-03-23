import viewModelBase = require("viewmodels/viewModelBase");
import database from "models/resources/database";
import shard from "models/resources/shard";
import { shardingTodo } from "common/developmentHelper";
import accessManager from "common/shell/accessManager";

abstract class shardViewModelBase extends viewModelBase {
    
    protected readonly db: database;
    
    protected constructor(db: database) {
        super();

        if (!db) {
            throw new Error("Database is required in " + this.constructor.name + " constructor. Did you forgot to set 'shardingMode' in router?");
        }
        
        this.db = db;

        this.isReadOnlyAccess =  ko.pureComputed(() => accessManager.default.readOnlyOrAboveForDatabase(this.db));
        this.isReadWriteAccessOrAbove = ko.pureComputed(() => accessManager.default.readWriteAccessOrAboveForDatabase(this.db));
        this.isAdminAccessOrAbove = ko.pureComputed(() => accessManager.default.adminAccessOrAboveForDatabase(this.db));
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
        shardingTodo("Marcin");
        return null;
    }
    
}

export = shardViewModelBase;
