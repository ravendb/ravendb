import database from "models/resources/database";
import shardedDatabase from "models/resources/shardedDatabase";
import DatabaseTopology = Raven.Client.ServerWide.DatabaseTopology;

class shard extends database {
    private readonly shardTopology: DatabaseTopology;
    readonly parent: shardedDatabase;
    private readonly _shardNumber: number;
    
    constructor(dbInfo: StudioDatabaseResponse, shardNumber: number, shardTopology: DatabaseTopology, parent: shardedDatabase) {
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
        
        const nodes = [
            ...this.shardTopology.Members,
            ...this.shardTopology.Rehabs,
            ...this.shardTopology.Promotables
        ];
        
        this.nodes(nodes);
        this.relevant(nodes.includes(this.clusterNodeTag()));
    }

    getLocations(): databaseLocationSpecifier[] {
        return this.nodes().map(x => ({
            nodeTag: x,
            shardNumber: this.shardNumber
        }));
    }
}

export = shard;
