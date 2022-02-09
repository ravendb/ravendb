import database from "models/resources/database";
import shardedDatabase from "models/resources/shardedDatabase";

class databaseShard extends database {
    parent: shardedDatabase;

    get group(): database {
        return this.parent;
    }
}

export = databaseShard;
