import database from "models/resources/database";
import databaseShard from "models/resources/databaseShard";

class shardedDatabase extends database {
    
    shards = ko.observableArray<databaseShard>([]);
    
    static isSharded(db: database): db is shardedDatabase {
        return db instanceof shardedDatabase;
    }

    get root(): database {
        return this;
    }
}

export = shardedDatabase;
