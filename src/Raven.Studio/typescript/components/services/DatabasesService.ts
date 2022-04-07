/// <reference path="../../../typings/tsd.d.ts" />

import getDatabasesCommand from "commands/resources/getDatabasesCommand";

export default class DatabasesService {
    
    async getDatabases() {
        return new getDatabasesCommand()
            .execute();
    }
    
}
