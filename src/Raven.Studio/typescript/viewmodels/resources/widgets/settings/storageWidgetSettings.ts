import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import storageKeyProvider = require("common/storage/storageKeyProvider");

class storageWidgetSettings extends dialogViewModelBase {

    static localStorageName = storageKeyProvider.storageKeyFor("storageWidgetScaleSettings");
    scaleDriveSize = ko.observable<boolean>();

    activate() {
        const savedSettings: boolean = localStorage.getObject(storageWidgetSettings.localStorageName);
        this.scaleDriveSize(savedSettings);
    }

    compositionComplete() {
        super.compositionComplete();

        $('.storage-widget-settings [data-toggle="tooltip"]').tooltip();
    }

    saveSettings() {
        localStorage.setObject(storageWidgetSettings.localStorageName, this.scaleDriveSize());
        this.close();
    }
}

export = storageWidgetSettings;
