import database from "models/resources/database";
import shardedDatabase from "models/resources/shardedDatabase";
import DatabaseUtils from "../../components/utils/DatabaseUtils";

class shard extends database {
    readonly parent: shardedDatabase;

    constructor(dbInfo: Raven.Client.ServerWide.Operations.DatabaseInfo, parent: shardedDatabase) {
        super(dbInfo, parent.clusterNodeTag);
        this.parent = parent;
    }

    get root(): database {
        return this.parent;
    }
    
    get shardNumber() {
        return DatabaseUtils.shardNumber(this.name);
    }
    
    get shardName() {
        return "Shard #" + this.shardNumber;
    }

    getLocations(): databaseLocationSpecifier[] {
        return this.nodes().map(x => ({
            nodeTag: x,
            shardNumber: this.shardNumber
        }));
    }
}

export = shard;
