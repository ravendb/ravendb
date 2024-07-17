import database from "models/resources/database";
import shardedDatabase from "models/resources/shardedDatabase";
import NodesTopology = Raven.Client.ServerWide.Operations.NodesTopology;
import StudioDatabaseInfo = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabases.StudioDatabaseInfo;

class shard extends database {
    private readonly shardTopology: NodesTopology;
    readonly parent: shardedDatabase;
    private readonly _shardNumber: number;
    
    constructor(dbInfo: StudioDatabaseInfo, shardNumber: number, shardTopology: NodesTopology, parent: shardedDatabase) {
        super(dbInfo, parent.clusterNodeTag);
        this.parent = parent;
        this.shardTopology = shardTopology;
        this._shardNumber = shardNumber;
        
        this.updateUsing(dbInfo);
    }

    isSharded(): this is shardedDatabase {
        return false;
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
    
    updateUsing(incomingCopy: StudioDatabaseInfo) {
        super.updateUsing(incomingCopy);
        
        this.name = incomingCopy.Name + "$" + this._shardNumber;

        const topology = incomingCopy.Sharding.Shards[this._shardNumber];
        
        const nodes = [
            ...topology.Members.map(x => this.mapNode(topology, x, "Member")),
            ...topology.Promotables.map(x => this.mapNode(topology, x, "Promotable")),
            ...topology.Rehabs.map(x => this.mapNode(topology, x, "Rehab")),
        ];
        
        this.nodes(nodes);
        this.relevant(nodes.some(x => x.tag === this.clusterNodeTag()));
        this.fixOrder(topology.PriorityOrder.length > 0);
    }

    getLocations(): databaseLocationSpecifier[] {
        return this.nodes().map(x => ({
            nodeTag: x.tag,
            shardNumber: this.shardNumber
        }));
    }
}

export = shard;
