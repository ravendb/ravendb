import resource = require("models/resource");

class filesystem extends resource {
    isDefault = false;
    statistics = ko.observable<filesystemStatisticsDto>();
    filesCount = ko.computed(() => this.statistics() ? this.statistics().FileCount : 0);
    files = ko.observableArray<filesystemFileHeaderDto>();

    constructor(public name: string) {
        super(name, "filesystem");
       
    }

    activate() {
        ko.postbox.publish("ActivateFilesystem", this);
    }
}
export = filesystem;