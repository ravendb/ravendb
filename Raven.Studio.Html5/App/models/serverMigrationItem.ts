import database = require("models/database");

class serverMigrationItem {

	resource: database;
	incremental = ko.observable<boolean>(true);
	stripReplicationInformation = ko.observable<boolean>(false);
	shouldDisableVersioningBundle = ko.observable<boolean>(false);

	constructor(database: database) {
		this.resource = database;
	}

	toDto(): serverMigrationItemDto {
		return {
			Name: this.resource.name,
			Incremental: this.incremental(),
			StripReplicationInformation: this.stripReplicationInformation(),
			ShouldDisableVersioningBundle: this.shouldDisableVersioningBundle()
		}
	}
    
}

export = serverMigrationItem;