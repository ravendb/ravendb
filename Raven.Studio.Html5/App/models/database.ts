import resource = require("models/resource");

class database extends resource {
    statistics = ko.observable<databaseStatisticsDto>();

    constructor(public name: string) {
        super(name);
        this.itemCount = ko.computed(() => this.statistics() ? this.statistics().CountOfDocuments : 0);
    }

	activate() {
		ko.postbox.publish("ActivateDatabase", this);
    }
}

export = database;