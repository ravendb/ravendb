import database from "models/resources/database";
import shard from "models/resources/shard";
import StudioDatabaseInfo = Raven.Server.Web.System.StudioDatabasesHandler.StudioDatabaseInfo;

class shardedDatabase extends database {
    
    shards = ko.observableArray<shard>([]);

    isSharded(): true {
        return true;
    }
    
    constructor(dbInfo: StudioDatabaseInfo, clusterNodeTag: KnockoutObservable<string>) {
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
                    nodeTag: node.tag,
                    shardNumber: shard.shardNumber
                })
            })
        })
        
        return locationSpecifiers;
    }

    updateUsing(incomingCopy: StudioDatabaseInfo) {
        super.updateUsing(incomingCopy);

        const topology = incomingCopy.Sharding.Orchestrator.NodesTopology;

        const nodes = [
            ...topology.Members.map(x => this.mapNode(topology, x, "Member")),
            ...topology.Promotables.map(x => this.mapNode(topology, x, "Member")),
            ...topology.Rehabs.map(x => this.mapNode(topology, x, "Member")),
        ];

        this.nodes(nodes);
        const nodeTag = this.clusterNodeTag();
        this.relevant(nodes.some(x => x.tag === nodeTag));

        const shards = Object.entries(incomingCopy.Sharding.Shards).map((kv) => {
            const [shardNumber, shardTopology] = kv;

            return new shard(incomingCopy, parseInt(shardNumber, 10), shardTopology, this);
        })
        this.shards(shards);
        this.relevant(nodes.some(x => x.tag === this.clusterNodeTag()));
    }
}

export = shardedDatabase;
