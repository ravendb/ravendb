import database from "models/resources/database";
import shardedDatabase from "models/resources/shardedDatabase";
import NodesTopology = Raven.Client.ServerWide.Operations.NodesTopology;

class shard extends database {
    private readonly shardTopology: NodesTopology;
    readonly parent: shardedDatabase;
    private readonly _shardNumber: number;
    
    constructor(dbInfo: StudioDatabaseResponse, shardNumber: number, shardTopology: NodesTopology, parent: shardedDatabase) {
        super(dbInfo, parent.clusterNodeTag);
        this.parent = parent;
        this.shardTopology = shardTopology;
        this._shardNumber = shardNumber;
        
        this.updateUsing(dbInfo);
    }

    get root(): database {
        return this.parent;
    }
    
    get shardNumber() {
        return this._shardNumber;
    }
    
    get shardName() {
        return "Shard #" + this.shardNumber;
    }
    
    updateUsing(incomingCopy: StudioDatabaseResponse) {
        super.updateUsing(incomingCopy);
        
        this.name = incomingCopy.DatabaseName + "$" + this._shardNumber;

        const topology = incomingCopy.Sharding.Shards[this._shardNumber];
        
        const nodes = [
            ...topology.Members.map(x => this.mapNode(topology, x, "Member")),
            ...topology.Promotables.map(x => this.mapNode(topology, x, "Member")),
            ...topology.Rehabs.map(x => this.mapNode(topology, x, "Member")),
        ];
        
        this.nodes(nodes);
        this.relevant(nodes.some(x => x.tag === this.clusterNodeTag()));
    }

    getLocations(): databaseLocationSpecifier[] {
        return this.nodes().map(x => ({
            nodeTag: x.tag,
            shardNumber: this.shardNumber
        }));
    }
}

export = shard;
