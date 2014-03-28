import resource = require("models/resource");

class filesystem extends resource {
    isDefault = false;
    statistics = ko.observable<filesystemStatisticsDto>();    
    files = ko.observableArray<filesystemFileHeaderDto>();

    constructor(public name: string) {
        super(name, 'filesystem');
        this.itemCount = ko.computed(() => this.statistics() ? this.statistics().FileCount : 0);
    }

    activate() {
        ko.postbox.publish("ActivateFilesystem", this);
    }

    static getNameFromUrl(url: string) {
        var index = url.indexOf("filesystems/");
        return (index > 0) ? url.substring(index + 10) : "";
    }
}
export = filesystem;