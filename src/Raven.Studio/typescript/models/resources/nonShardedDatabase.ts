import database from "models/resources/database";

class nonShardedDatabase extends database {
    get root(): database {
        return this;
    }
}

export = nonShardedDatabase;
