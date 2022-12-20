import database from "models/resources/database";
import shard from "models/resources/shard";

class shardedDatabase extends database {
    
    shards = ko.observableArray<shard>([]);
    
    static isSharded(db: database): db is shardedDatabase {
        return db instanceof shardedDatabase;
    }
    
    constructor(dbInfo: StudioDatabaseResponse, clusterNodeTag: KnockoutObservable<string>) {
        super(dbInfo, clusterNodeTag);
        
        this.updateUsing(dbInfo);
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

    updateUsing(incomingCopy: StudioDatabaseResponse) {
        super.updateUsing(incomingCopy);

        const topology = incomingCopy.Sharding.Orchestrator.Topology;

        const nodes = [
            ...topology.Members,
            ...topology.Promotables,
            ...topology.Rehabs
        ];

        this.nodes(nodes);
        const nodeTag = this.clusterNodeTag();
        this.relevant(nodes.includes(nodeTag));

        const shards = Object.entries(incomingCopy.Sharding.Shards).map((kv) => {
            const [shardNumber, shardTopology] = kv;

            return new shard(incomingCopy, parseInt(shardNumber, 10), shardTopology, this);
        })
        this.shards(shards);
        this.relevant(nodes.includes(this.clusterNodeTag()));
    }
}

export = shardedDatabase;
