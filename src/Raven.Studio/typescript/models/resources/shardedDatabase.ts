import database from "models/resources/database";
import shard from "models/resources/shard";
import DatabaseUtils from "../../components/utils/DatabaseUtils";

class shardedDatabase extends database {
    
    shards = ko.observableArray<shard>([]);
    
    static isSharded(db: database): db is shardedDatabase {
        return db instanceof shardedDatabase;
    }
    
    constructor(dbInfo: Raven.Client.ServerWide.Operations.DatabaseInfo[], clusterNodeTag: KnockoutObservable<string>) {
        super(dbInfo[0], clusterNodeTag);
        
        this.updateUsingGroup(dbInfo);
    }

    get root(): database {
        return this;
    }

    getLocations(): databaseLocationSpecifier[] {
        const locationSpecifiers: databaseLocationSpecifier[] = [];
        
        this.shards().forEach(shard => {
            shard.nodes().forEach(node => {
                locationSpecifiers.push({
                    nodeTag: node,
                    shardNumber: shard.shardNumber
                })
            })
        })
        
        return locationSpecifiers;
    }

    updateUsingGroup(dbs: Raven.Client.ServerWide.Operations.DatabaseInfo[]) {
        // update shared info
        this.updateUsing(dbs[0]);
        
        this.shards(dbs.map(db => new shard(db, this)));
        
        const nodes = this.shards().flatMap(x => x.nodes());
        
        // in sharded environment db is relevant if any shard exists on local node
        this.relevant(!!nodes.find(x => x === this.clusterNodeTag()));
        this.name = DatabaseUtils.shardGroupKey(dbs[0].Name);
    }
}

export = shardedDatabase;
