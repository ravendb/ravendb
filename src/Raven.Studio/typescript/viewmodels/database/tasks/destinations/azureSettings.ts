import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");
import genUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");

class azureSettings extends backupSettings {
    storageContainer = ko.observable<string>();
    remoteFolderName = ko.observable<string>();
    accountName = ko.observable<string>();
    accountKey = ko.observable<string>();
    sasToken = ko.observable<string>();

    targetOperation: string;

    constructor(dto: Raven.Client.Documents.Operations.Backups.AzureSettings, targetOperation: string) {
        super(dto, "Azure");

        this.storageContainer(dto.StorageContainer);
        this.remoteFolderName(dto.RemoteFolderName || "");
        this.accountName(dto.AccountName);
        this.accountKey(dto.AccountKey);
        this.sasToken(dto.SasToken);

        this.targetOperation = targetOperation;

        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.storageContainer,
            this.remoteFolderName,
            this.accountName,
            this.accountKey,
            this.configurationScriptDirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    compositionComplete(view: Element, container: HTMLElement) {
        popoverUtils.longWithHover($(".storage-container-info", container),
            {
                content: tasksCommonContent.textForPopover("Storage container", this.targetOperation)
            });
    }

    initValidation() {
        /* Container name must:
            - start with a letter or number, and can contain only letters, numbers, and the dash (-) character.
            - every dash (-) character must be immediately preceded and followed by a letter or number; 
            - consecutive dashes are not permitted in container names.
            - all letters in a container name must be lowercase.
            - container names must be from 3 through 63 characters long.
        */
        const allowedCharactersRegExp = /^[a-z0-9-]+$/;
        const twoDashesRegExp = /--/;
        this.storageContainer.extend({
            validation: [
                {
                    validator: (storageContainer: string) => storageContainer && storageContainer.length >= 3 && storageContainer.length <= 63,
                    message: "Container name should be between 3 and 63 characters long"
                },
                {
                    validator: (storageContainer: string) => allowedCharactersRegExp.test(storageContainer),
                    message: "Allowed characters lowercase characters, numbers and dashes"
                },
                {
                    validator: (storageContainer: string) => 
                        storageContainer && storageContainer[0] !== "-" && storageContainer[storageContainer.length - 1] !== "-",
                    message: "Container name must start and end with a letter or a number"
                },
                {
                    validator: (storageContainer: string) => !twoDashesRegExp.test(storageContainer),
                    message: "Consecutive dashes are not permitted in container names"
                }
            ]
        });

        this.accountName.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.accountKey.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.localConfigValidationGroup = ko.validatedObservable({
            storageContainer: this.storageContainer,
            accountName: this.accountName,
            accountKey: this.accountKey
        });
    }

    toDto(): Raven.Client.Documents.Operations.Backups.AzureSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.AzureSettings;
        dto.StorageContainer = this.storageContainer();
        dto.RemoteFolderName = this.remoteFolderName();
        dto.AccountName = this.accountName();
        dto.AccountKey = this.accountKey();
        dto.SasToken = this.sasToken();

        return genUtils.trimProperties(dto, ["RemoteFolderName", "AccountName"]);
    }

    static empty(targetOperation: string): azureSettings {
        return new azureSettings({
            Disabled: true,
            StorageContainer: null,
            RemoteFolderName: null,
            AccountName: null,
            AccountKey: null,
            SasToken: null,
            GetBackupConfigurationScript: null
        }, targetOperation);
    }
}

export = azureSettings;
