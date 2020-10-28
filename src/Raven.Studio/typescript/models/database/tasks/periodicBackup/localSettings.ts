import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");
import genUtils = require("common/generalUtils");

class localSettings extends backupSettings {
    folderPath = ko.observable<string>();
    folderPathHasFocus = ko.observable<boolean>(false);

    constructor(dto: Raven.Client.Documents.Operations.Backups.LocalSettings) {
        super(dto, "Local");

        this.folderPath(dto.FolderPath);

        this.initValidation();
        
        _.bindAll(this, "localBackupPathChanged");
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.folderPath,
            this.configurationScriptDirtyFlag().isDirty 
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    initValidation() {
        this.folderPath.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.localConfigValidationGroup = ko.validatedObservable({
            folderPath: this.folderPath
        });
    }

    localBackupPathChanged(value: string) {
        this.folderPath(value);

        // try to continue autocomplete flow
        this.folderPathHasFocus(true);
    }

    toDto(): Raven.Client.Documents.Operations.Backups.LocalSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.LocalSettings;
        dto.FolderPath = this.folderPath();

        return genUtils.trimProperties(dto, ["FolderPath"]);
    }

    static empty(): localSettings {
        return new localSettings({
            Disabled: true,
            FolderPath: null,
            GetBackupConfigurationScript: null
        });
    }
}

export = localSettings;
