import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

class storageWidgetSettings extends dialogViewModelBase {

    view = require("views/resources/widgets/settings/storageWidgetSettings.html");
    
    scaleDriveSize = ko.observable<boolean>();

    constructor(scaleToSize: boolean) {
        super();
        
        this.scaleDriveSize(scaleToSize);
    }

    compositionComplete() {
        super.compositionComplete();

        $('.storage-widget-settings [data-toggle="tooltip"]').tooltip();
    }

    saveSettings() {
        dialog.close(this, this.scaleDriveSize());
    }
}

export = storageWidgetSettings;
