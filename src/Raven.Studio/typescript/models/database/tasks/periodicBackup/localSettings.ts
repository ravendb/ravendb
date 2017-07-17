import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");

class localSettings extends backupSettings {
    folderPath = ko.observable<string>();

    availableDriveNames = ko.observableArray<string>();
    databaseDriveNames = ko.observableArray<string>();

    displaySameDriveWarning: KnockoutComputed<boolean>;

    constructor(dto: Raven.Client.Server.PeriodicBackup.LocalSettings) {
        super(dto);

        this.folderPath(dto.FolderPath);

        this.connectionType = "Local";
        this.initValidation();

        this.displaySameDriveWarning = ko.pureComputed(() => {
            if (!this.folderPath())
                return false;

            var driveName = this.folderPath().toLowerCase().substr(0, 3);
            return !!this.databaseDriveNames().find(x => x === driveName);
        });
    }

    initValidation() {
        this.folderPath.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.validationGroup = ko.validatedObservable({
            folderPath: this.folderPath
        });
    }

    updateDrivesInfo(drivesInfo: Raven.Server.Documents.PeriodicBackup.DrivesInfo) {
        this.availableDriveNames(drivesInfo.AllDriveNames);
        this.databaseDriveNames(drivesInfo.DatabaseDriveNames);
    }

    toDto(): Raven.Client.Server.PeriodicBackup.LocalSettings {
        const dto = super.toDto() as Raven.Client.Server.PeriodicBackup.LocalSettings;
        dto.FolderPath = this.folderPath();
        return dto;
    }

    static empty(): localSettings {
        return new localSettings({
            Disabled: true,
            FolderPath: null
        });
    }
}

export = localSettings;