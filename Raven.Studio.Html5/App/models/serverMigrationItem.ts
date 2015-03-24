import database = require("models/database");

class serverMigrationItem {

	resource: database;

	constructor(database: database) {
		this.resource = database;
	}

	toDto(): serverMigrationItemDto {
		return {
			Name: this.resource.name
		}
	}
    
}

export = serverMigrationItem;