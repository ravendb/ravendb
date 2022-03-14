import viewModelBase = require("viewmodels/viewModelBase");
import shard = require("models/resources/shard");
import activator = require("durandal/activator");
import database = require("models/resources/database");
import shardedDatabase = require("models/resources/shardedDatabase");
import shardViewModelBase = require("viewmodels/shardViewModelBase");
import shardSelector = require("viewmodels/common/sharding/shardSelector");
import allShardsDialog = require("viewmodels/common/sharding/allShardsDialog");
import nonShardedDatabase from "models/resources/nonShardedDatabase";
import { shardingTodo } from "common/developmentHelper";

class shardAwareContainer extends viewModelBase {
    protected rootActivator: DurandalActivator<any>;
    private readonly childCtr: new (db: database, state?: any) => shardViewModelBase;
    
    mode: shardingMode;
    shards = ko.observableArray<shard>();
    /**
     * this.activeDatabase represents database from url: it might be single shard (ex. db1$5) or all shards (ex. db1)
     * effective database represents database for which we display view 
     */
    effectiveDatabase = ko.observable<database>();

    activationData: any;

    child = ko.observable<shardViewModelBase>();
    view = require("views/common/sharding/shardAwareContainer.html");
    
    shardSelector = ko.observable<shardSelector>();
    allShardsDialog = ko.observable<allShardsDialog>();
    
    showContext: KnockoutComputed<boolean>;
    canCloseContext: KnockoutComputed<boolean>;
    canChangeScope: KnockoutComputed<boolean>;
    contextName: KnockoutComputed<string>;
    pinned: KnockoutComputed<boolean>;

    constructor(mode: shardingMode, childCtr: new (db: database, state?: any) => shardViewModelBase) {
        super();
        
        this.rootActivator = activator.create();
        
        this.mode = mode;
        this.childCtr = childCtr;
        
        this.bindToCurrentInstance("useDatabase", "exit" ,"useAllShards");
        
        this.showContext = ko.pureComputed(() => {
            if (this.activeDatabase() instanceof nonShardedDatabase) {
                return false;
            }

            if (this.allShardsDialog() || this.shardSelector()) {
                return false;
            }
            
            if (this.mode === "allShardsOnly" && this.activeDatabase() instanceof shardedDatabase) {
                return false;
            }
            
            return true;
        });
        
        this.canCloseContext = ko.pureComputed(() => {
            if (this.activeDatabase() instanceof shard) {
                return true;
            }
            
            return this.effectiveDatabase() && this.effectiveDatabase() instanceof shard;
        });
        
        this.canChangeScope = ko.pureComputed(() => this.mode !== "allShardsOnly");
        
        this.contextName = ko.pureComputed(() => {
            const db = this.effectiveDatabase();

            if (!db) {
                return "";
            }

            if (db instanceof shardedDatabase) {
                return "All Shards";
            }

            if (db instanceof shard) {
                return db.shardName;
            }
            
            return "";
        });
        
        this.pinned = ko.pureComputed(() => this.activeDatabase() && this.activeDatabase() instanceof shard);
    }
    
    activate(args: any, parameters?: any) {
        super.activate(args, parameters);
        
        this.activationData = args;
    }

    compositionComplete() {
        super.compositionComplete();
        
        const activeDatabase = this.activeDatabase();
        const shards = (activeDatabase.root instanceof shardedDatabase) ? activeDatabase.root.shards() : [];
        this.shards.push(...shards);
        
        this.resetView();
    }
    
    //TODO: candeactivate?
    
    deactivate() {
        super.deactivate();

        if (this.child()) {
            this.child(null);
            return this.rootActivator.deactivate(true);
        }
    }

    private routeToAllShards() {
        if (this.activeDatabase() instanceof shardedDatabase) {
            console.warn("Already using shardedDatabase. Ignoring route request");
            return;
        }
        const allShards = this.activeDatabase().root;
        this.databasesManager.activate(allShards);
    }

    changeScope() {
        this.resetView(true);
    }
    
    private onDatabaseSelected(db: database, pin: boolean) {
        const dbChanged = db.name !== this.activeDatabase().name;
        
        if (!dbChanged) {
            this.useDatabase(db);
            this.shardSelector(null);
            return;
        }
        
        if (pin) {
            this.databasesManager.activate(db);
            this.shardSelector(null);
        } else {
            shardingTodo("Marcin");
            this.useDatabase(db); //TODO: 
            this.shardSelector(null);    
        }
    }
    
    useDatabase(db: database) {
        shardingTodo("Marcin");
        // TODO: allow to persist between views - check if local / remote
        //TODO: doesn't work when pinning
        const oldChild = this.child();
        const oldViewState = oldChild?.getViewState?.();
        
        this.effectiveDatabase(db);
        this.activateChildView(db); //TODO: old child state!
    }

    useAllShards() {
        const allShards = this.activeDatabase().root;
        this.shardSelector(null);
        this.allShardsDialog(null);
        this.useDatabase(allShards);
    }
    
    supportsDatabase(db: database): db is shard {
        if (db instanceof nonShardedDatabase) {
            return true;
        }
        
        switch (this.mode) {
            case "singleShardOnly":
                return db && db instanceof shard;
            case "both":
                return true;
            case "allShardsOnly":
                return db && !(db instanceof shard);
        }
        
        return false;
    }
    
    private activateChildView(db: database) {
        const child = new this.childCtr(db);
        
        this.rootActivator.activateItem(child, this.activationData)
            .done(() => {
                this.child(child);
            })
        //TODO: handle failure!
    }
    
    private resetView(forceShardSelection = false) {
        const activeDatabase = this.activeDatabase();
        
        if (this.supportsDatabase(activeDatabase) && !forceShardSelection) {
            this.effectiveDatabase(activeDatabase);
            this.activateChildView(activeDatabase);
        } else if (this.mode === "allShardsOnly" && activeDatabase instanceof shard) {
            this.allShardsDialog(new allShardsDialog(() => this.routeToAllShards(), () => this.useAllShards()));
        } else {
            this.shardSelector(new shardSelector(this.shards(), (db, pin) => this.onDatabaseSelected(db, pin)));
        }
    }
    
    exit() {
        if (this.activeDatabase() instanceof shard) {
            // we have pin - so let's remove it
            this.routeToAllShards();
        } else {
            // we have all shards global context
            this.resetView();
        }
    }
}

export = shardAwareContainer;
