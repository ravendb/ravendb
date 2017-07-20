import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");

class azureSettings extends backupSettings {
    storageContainer = ko.observable<string>();
    remoteFolderName = ko.observable<string>();
    accountName = ko.observable<string>();
    accountKey = ko.observable<string>();

    constructor(dto: Raven.Client.Server.PeriodicBackup.AzureSettings) {
        super(dto);

        this.storageContainer(dto.StorageContainer);
        this.remoteFolderName(dto.RemoteFolderName);
        this.accountName(dto.AccountName);
        this.accountKey(dto.AccountKey);

        this.connectionType = "Azure";
        this.initValidation();
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
                    validator: (storageContainer: string) => this.validate(() => !!storageContainer && storageContainer.length >= 4 && storageContainer.length <= 64),
                    message: "Container name must be at least 3 characters long and no more than 63"
                },
                {
                    validator: (storageContainer: string) => this.validate(() => allowedCharactersRegExp.test(storageContainer)),
                    message: "Allowed characters are a-z, 0-9 and '-' (hyphen)"
                },
                {
                    validator: (storageContainer: string) => this.validate(() => !!storageContainer && storageContainer[0] !== "-" && storageContainer[storageContainer.length - 1] !== "-"),
                    message: "Container name must start and end with a letter or number"
                },
                {
                    validator: (storageContainer: string) => this.validate(() => !twoDashesRegExp.test(storageContainer)),
                    message: "Consecutive dashes are not permitted in container names"
                }
            ]
        });

        this.remoteFolderName.extend({
            required: {
                onlyIf: () => this.enabled()
            }
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

        this.validationGroup = ko.validatedObservable({
            storageContainer: this.storageContainer,
            remoteFolderName: this.remoteFolderName,
            AccountName: this.accountName,
            AccountKey: this.accountKey
        });

        this.credentialsValidationGroup = ko.validatedObservable({
            AccountName: this.accountName,
            AccountKey: this.accountKey
        });
    }

    toDto(): Raven.Client.Server.PeriodicBackup.AzureSettings {
        const dto = super.toDto() as Raven.Client.Server.PeriodicBackup.AzureSettings;
        dto.StorageContainer = this.storageContainer();
        dto.RemoteFolderName = this.remoteFolderName();
        dto.AccountName = this.accountName();
        dto.AccountKey = this.accountKey();
        return dto;
    }

    static empty(): azureSettings {
        return new azureSettings({
            Disabled: true,
            StorageContainer: null,
            RemoteFolderName: null,
            AccountName: null,
            AccountKey: null
        });
    }
}

export = azureSettings;