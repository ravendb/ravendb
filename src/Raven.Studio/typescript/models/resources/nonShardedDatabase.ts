import database from "models/resources/database";

class nonShardedDatabase extends database {
    get root(): database {
        return this;
    }
    
    getLocations(): databaseLocationSpecifier[] {
        return this.nodes().map(x => ({
            nodeTag: x
        }));
    }
}

export = nonShardedDatabase;
