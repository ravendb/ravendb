import database from "models/resources/database";
import shardedDatabase from "models/resources/shardedDatabase";

class databaseShard extends database {
    parent: shardedDatabase;

    get root(): database {
        return this.parent;
    }
}

export = databaseShard;
