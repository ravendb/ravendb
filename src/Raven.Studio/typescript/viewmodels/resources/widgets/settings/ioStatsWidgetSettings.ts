import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

class ioStatsWidgetSettings extends dialogViewModelBase {

    splitIops = ko.observable<boolean>();
    splitThroughput = ko.observable<boolean>();

    constructor(settings: Required<ioStatsWidgetConfig>) {
        super();
        
        this.splitIops(settings.splitIops);
        this.splitThroughput(settings.splitThroughput);
    }
    
    saveSettings() {
        const result: Required<ioStatsWidgetConfig> = {
            splitIops: this.splitIops(),
            splitThroughput: this.splitThroughput()
        };
        dialog.close(this, result);
    }
}

export = ioStatsWidgetSettings;
