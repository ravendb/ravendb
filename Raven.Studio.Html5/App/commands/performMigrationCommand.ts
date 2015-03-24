/// <reference path="../models/dto.ts" />

import commandBase = require("commands/commandBase");
import database = require("models/database");
 
class performMigrationCommand extends commandBase {

	constructor(private migration: serverMigrationDto, private db: database) { //TODO: pass on progess callback
        super();
    }

    execute(): JQueryPromise<any> {
		return this.post("/admin/serverMigration", JSON.stringify(this.migration), this.db);
		//TODO: extract oepration id

    }


}

export = performMigrationCommand;