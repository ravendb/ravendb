import database from "models/resources/database";
import shardedDatabase from "models/resources/shardedDatabase";

class shard extends database {
    parent: shardedDatabase;

    get root(): database {
        return this.parent;
    }
    
    get shardName() {
        return "Shard #" + this.name.split("$")[1];
    }
}

export = shard;
