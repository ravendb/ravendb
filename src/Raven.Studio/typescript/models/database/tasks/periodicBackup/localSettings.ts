import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");

class localSettings extends backupSettings {
    folderPath = ko.observable<string>();

    constructor(dto: Raven.Client.Documents.Operations.Backups.LocalSettings) {
        super(dto, "Local");

        this.folderPath(dto.FolderPath);

        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.folderPath
        ], false,  jsonUtil.newLineNormalizingHashFunction);
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

    toDto(): Raven.Client.Documents.Operations.Backups.LocalSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.LocalSettings;
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
