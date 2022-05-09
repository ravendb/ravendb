import viewModelBase = require("viewmodels/viewModelBase");
import shardSelector = require("viewmodels/common/sharding/shardSelector");
import nonShardedDatabase from "models/resources/nonShardedDatabase";
import shardedDatabase from "models/resources/shardedDatabase";
import shard from "models/resources/shard";
import database from "models/resources/database";
import genUtils from "common/generalUtils";

class shardingContext extends viewModelBase {

    mode: shardingMode;
    shards = ko.observableArray<shard>();

    effectiveLocation = ko.observable<databaseLocationSpecifier>(null);

    onChangeHandler: (db: database, location: databaseLocationSpecifier) => void;
    
    view = require("views/common/sharding/shardingContext.html");

    shardSelector = ko.observable<shardSelector>();

    showContext: KnockoutComputed<boolean>;
    canChangeScope: KnockoutComputed<boolean>;
    contextName: KnockoutComputed<string>;
    
    constructor(mode: shardingMode) {
        super();
        
        this.mode = mode;

        this.showContext = ko.pureComputed(() => {
            if (this.shardSelector()) {
                return false;
            }
            
            return !!this.effectiveLocation();
        });

        this.canChangeScope = ko.pureComputed(() => this.mode === "singleShard");

        this.contextName = ko.pureComputed(() => {
            const db = this.activeDatabase();

            if (!db) {
                return "";
            }

            if (this.effectiveLocation()) {
                return genUtils.formatLocation(this.effectiveLocation());
            }
            
            return "";
        });

        this.bindToCurrentInstance("useDatabase");
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        const activeDatabase = this.activeDatabase();
        const shards = (activeDatabase.root instanceof shardedDatabase) ? activeDatabase.root.shards() : [];
        this.shards.push(...shards);
    }
    
    onChange(handler: (db: database, location: databaseLocationSpecifier) => void) {
        this.onChangeHandler = handler;
    }

    changeScope() {
        this.resetView(true);
    }

    private onShardSelected(db: shard, nodeTag: string): void {
        const location: databaseLocationSpecifier = {
            shardNumber: db.shardNumber,
            nodeTag: nodeTag
        };

        this.useDatabase(db.root, location);
        this.shardSelector(null);
    }

    supportsDatabase(db: database): boolean {
        if (db instanceof nonShardedDatabase) {
            return true;
        }

        if (db instanceof shardedDatabase) {
            switch (this.mode) {
                case "allShards":
                    return !this.effectiveLocation();
                case "singleShard":
                    return !!this.effectiveLocation();
            }
        }
        
        return true;
    }

    resetView(forceShardSelection = false) {
        const activeDatabase = this.activeDatabase();

        this.effectiveLocation(null);
        
        if (this.supportsDatabase(activeDatabase) && !forceShardSelection) {
            this.onChangeHandler(activeDatabase, null);
        } else {
            this.shardSelector(new shardSelector(this.shards(), (db, nodeTag) => this.onShardSelected(db, nodeTag)));
        }
    }

    useDatabase(db: database, location: databaseLocationSpecifier) {
        this.effectiveLocation(location);
        this.onChangeHandler(db, location);
    }
}

export = shardingContext;
