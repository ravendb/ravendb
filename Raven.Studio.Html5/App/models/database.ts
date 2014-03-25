class database {
    isSystem = false;
    isSelected = ko.observable(false);
    statistics = ko.observable<databaseStatisticsDto>();
    docCount: KnockoutComputed<number>;
    isVisible = ko.observable(true);

    constructor(public name: string) {
        this.docCount = ko.computed(() => this.statistics() ? this.statistics().CountOfDocuments : 0);
    }

	activate() {
        ko.postbox.publish("ActivateDatabase", this);
    }

    static getNameFromUrl(url: string) {
        // for embedded studio we don't pass full url
        if (url.indexOf("http") < 0) {
            return url;
        }
        var index = url.indexOf("databases/");
        return (index > 0) ? url.substring(index + 10) : "";
    }
}

export = database;