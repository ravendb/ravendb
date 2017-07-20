import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");

class localSettings extends backupSettings {
    folderPath = ko.observable<string>();

    displaySameDriveWarning: KnockoutComputed<boolean>;

    constructor(dto: Raven.Client.Server.PeriodicBackup.LocalSettings) {
        super(dto);

        this.folderPath(dto.FolderPath);

        this.connectionType = "Local";
        this.initValidation();
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