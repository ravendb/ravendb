import resource = require("models/resource");

class filesystem extends resource {
    isDefault = false;
    statistics = ko.observable<filesystemStatisticsDto>();

    constructor(public name: string) {
        super(name);
        this.itemCount = ko.computed(() => this.statistics() ? this.statistics().FileCount : 0);
    }

    activate() {
        ko.postbox.publish("ActivateFilesystem", this);
    }
}
export = filesystem;