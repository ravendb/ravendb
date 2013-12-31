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
}

export = database; 