class filesystem {
    isSystem = false;
    isDefault = false;
    isSelected = ko.observable(false);
    statistics = ko.observable<filesystemStatisticsDto>();
    filesCount: KnockoutComputed<number>;
    isVisible = ko.observable(true);

    constructor(public name: string) {
        this.filesCount = ko.computed(() => this.statistics() ? this.statistics().FileCount : 0);
    }

    activate() {
        ko.postbox.publish("ActivateFilesystem", this);
    }
}

export = filesystem;