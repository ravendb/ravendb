import database from "models/resources/database";
import shardedDatabase from "models/resources/shardedDatabase";

class shard extends database {
    parent: shardedDatabase;

    get root(): database {
        return this.parent;
    }
    
    get shardNumber() {
        return parseInt(this.name.split("$")[1], 10);
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
