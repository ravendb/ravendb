import database from "models/resources/database";

class nonShardedDatabase extends database {
    get group(): database {
        return this;
    }
}

export = nonShardedDatabase;
