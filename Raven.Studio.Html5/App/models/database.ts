import resource = require("models/resource");

class database extends resource {
    statistics = ko.observable<databaseStatisticsDto>();
    activeBundles = ko.observableArray<string>();

    constructor(public name: string, private isDisabled: boolean = false, private bundles = []) {
        super(name, 'database');
        this.disabled(isDisabled);
        this.activeBundles(bundles);
        this.itemCount = ko.computed(() => this.statistics() ? this.statistics().CountOfDocuments : 0);
    }

	activate() {
        ko.postbox.publish("ActivateDatabase", this);
	}

    static getNameFromUrl(url: string) {
        var index = url.indexOf("databases/");
        return (index > 0) ? url.substring(index + 10) : "";
    }
}

export = database;