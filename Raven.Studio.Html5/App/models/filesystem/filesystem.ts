import resource = require("models/resource");

class filesystem extends resource {
    //isDefault = false;
    statistics = ko.observable<filesystemStatisticsDto>();    
    files = ko.observableArray<filesystemFileHeaderDto>();

    constructor(public name: string, isDisabled: boolean = false) {
        super(name, 'filesystem');
        this.disabled(isDisabled);
        this.itemCount = ko.computed(() => this.statistics() ? this.statistics().FileCount : 0);
        this.itemCountText = ko.computed(() => {
            var itemCount = this.itemCount();
            var text = itemCount + ' file';
            if (itemCount != 1) {
                text = text + 's';
            }
            return text;
        });
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