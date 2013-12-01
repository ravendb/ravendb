class database {
    isSystem = false;
    isSelected = ko.observable(false);
    statistics = ko.observable<documentStatistics>();
    docCount: KnockoutComputed<number>;

    constructor(public name: string) {
        this.docCount = ko.computed(() => this.statistics() ? this.statistics().CountOfDocuments : 0);
    }

	activate() {
		ko.postbox.publish("ActivateDatabase", this);
    }
}

export = database; 