import database from "models/resources/database";

class nonShardedDatabase extends database {
    get root(): database {
        return this;
    }

    constructor(dbInfo: StudioDatabaseResponse, clusterNodeTag: KnockoutObservable<string>) {
        super(dbInfo, clusterNodeTag);
        
        this.updateUsing(dbInfo);
    }
    
    getLocations(): databaseLocationSpecifier[] {
        return this.nodes().map(x => ({
            nodeTag: x
        }));
    }

    updateUsing(incomingCopy: StudioDatabaseResponse) {
        super.updateUsing(incomingCopy);
        
        const topology = incomingCopy.Topology;
        const nodeTag = this.clusterNodeTag();
        
        const nodes = [
            ...topology.Members,
            ...topology.Promotables,
            ...topology.Rehabs
        ];
        
        this.nodes(nodes);
        this.relevant(nodes.includes(nodeTag));
    }
}

export = nonShardedDatabase;
