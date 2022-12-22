import database from "models/resources/database";
import StudioDatabaseInfo = Raven.Server.Web.System.StudioDatabasesHandler.StudioDatabaseInfo;

class nonShardedDatabase extends database {
    get root(): database {
        return this;
    }

    get isSharded(): boolean {
        return false;
    }

    constructor(dbInfo: StudioDatabaseInfo, clusterNodeTag: KnockoutObservable<string>) {
        super(dbInfo, clusterNodeTag);
        
        this.updateUsing(dbInfo);
    }
    
    getLocations(): databaseLocationSpecifier[] {
        return this.nodes().map(x => ({
            nodeTag: x.tag
        }));
    }

    updateUsing(incomingCopy: StudioDatabaseInfo) {
        super.updateUsing(incomingCopy);
        
        const topology = incomingCopy.NodesTopology;
        const nodeTag = this.clusterNodeTag();
        
        const nodes = [
            ...topology.Members.map(x => this.mapNode(topology, x, "Member")),
            ...topology.Promotables.map(x => this.mapNode(topology, x, "Member")),
            ...topology.Rehabs.map(x => this.mapNode(topology, x, "Member")),
        ];
        
        this.nodes(nodes);
        this.relevant(nodes.some(x => x.tag === nodeTag));
    }
}

export = nonShardedDatabase;
