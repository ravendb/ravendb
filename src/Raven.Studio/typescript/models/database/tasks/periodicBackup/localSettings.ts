import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");
import genUtils = require("common/generalUtils");

type container =  "backup" | "connectionString";

class localSettings extends backupSettings {
    folderPath = ko.observable<string>();
    folderPathHasFocus = ko.observable<boolean>(false);
    
    container = ko.observable<container>();
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.LocalSettings, localSettingsFor: container) {
        super(dto, "Local");

        this.folderPath(dto.FolderPath);
        this.container(localSettingsFor);

        this.initValidation();
        
        _.bindAll(this, "localPathChanged");
        
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

    localPathChanged(value: string) {
        this.folderPath(value);

        // try to continue autocomplete flow
        this.folderPathHasFocus(true);
    }

    toDto(): Raven.Client.Documents.Operations.Backups.LocalSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.LocalSettings;
        dto.FolderPath = this.folderPath();

        return genUtils.trimProperties(dto, ["FolderPath"]);
    }

    static empty(localSettingsFor: container): localSettings {
        return new localSettings({
            Disabled: true,
            FolderPath: null,
            GetBackupConfigurationScript: null
        }, localSettingsFor);
    }
}

export = localSettings;
