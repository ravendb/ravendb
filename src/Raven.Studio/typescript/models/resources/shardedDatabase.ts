import database from "models/resources/database";
import shard from "models/resources/shard";

class shardedDatabase extends database {
    
    shards = ko.observableArray<shard>([]);
    
    static isSharded(db: database): db is shardedDatabase {
        return db instanceof shardedDatabase;
    }

    get root(): database {
        return this;
    }
}

export = shardedDatabase;
