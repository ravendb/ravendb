import amazonSettings = require("models/database/tasks/periodicBackup/amazonSettings");
import jsonUtil = require("common/jsonUtil");
import genUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");

class glacierSettings extends amazonSettings {
    vaultName = ko.observable<string>();

    targetOperation: string;
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.GlacierSettings, allowedRegions: Array<string>, targetOperation: string) {
        super(dto, "Glacier", allowedRegions);

        this.vaultName(dto.VaultName);
        
        this.targetOperation = targetOperation;
        
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.vaultName,
            this.awsAccessKey,
            this.awsSecretKey,
            this.awsRegionName,
            this.remoteFolderName,
            this.selectedAwsRegion,
            this.configurationScriptDirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    compositionComplete(view: Element, container: HTMLElement) {
        popoverUtils.longWithHover($(".vault-info"),
            {
                content: tasksCommonContent.textForPopover("Vault", this.targetOperation)
            });
    }
    
    initValidation() {
        // - vault name can be between 1 and 255 characters long.
        // - allowed characters are a-z, A-Z, 0-9, '_' (underscore), '-' (hyphen), and '.' (period).
        const regExp = /^[A-Za-z0-9_\.-]+$/;

        this.vaultName.extend({
            validation: [
                {
                    validator: (vaultName: string) => vaultName && vaultName.length >= 1 && vaultName.length <= 255,
                    message: "Vault name must be at least 1 character and no more than 255 characters long"
                },
                {
                    validator: (vaultName: string) => regExp.test(vaultName),
                    message: "Allowed characters are a-z, A-Z, 0-9, '_' (underscore), '-' (hyphen), and '.' (period)"
                }
            ]
        });

        this.localConfigValidationGroup = ko.validatedObservable({
            awsAccessKey: this.awsAccessKey,
            awsSecretKey: this.awsSecretKey,
            awsRegionName: this.awsRegionName,
            vaultName: this.vaultName
        });
    }

    toDto(): Raven.Client.Documents.Operations.Backups.GlacierSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.GlacierSettings;
        dto.VaultName = this.vaultName();

        return genUtils.trimProperties(dto, ["RemoteFolderName", "AwsRegionName", "AwsAccessKey"]);
    }

    static empty(allowedRegions: Array<string>, targetOperation: string): glacierSettings {
        return new glacierSettings({
            Disabled: true,
            AwsAccessKey: null,
            AwsRegionName: null,
            AwsSecretKey: null,
            AwsSessionToken: null,
            RemoteFolderName: null,
            VaultName: null,
            GetBackupConfigurationScript: null
        }, allowedRegions, targetOperation);
    }
}

export = glacierSettings;
