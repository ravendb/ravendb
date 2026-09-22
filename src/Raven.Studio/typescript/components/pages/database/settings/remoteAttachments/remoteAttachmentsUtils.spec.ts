import { remoteAttachmentsUtils } from "./remoteAttachmentsUtils";

type RemoteAttachmentsAzureSettings = Raven.Client.Documents.Attachments.RemoteAttachmentsAzureSettings;
type RemoteAttachmentsConfiguration = Raven.Client.Documents.Attachments.RemoteAttachmentsConfiguration;

const destinationIdentifier = "azure-dest";

const azureSettingsBase: RemoteAttachmentsAzureSettings = {
    AccountName: "account",
    AccountKey: null,
    SasToken: null,
    StorageContainer: "container",
    RemoteFolderName: "folder",
};

const sasToken = "sv=2024-01-01&sig=abc";

function createConfiguration(azureSettings: RemoteAttachmentsAzureSettings): RemoteAttachmentsConfiguration {
    return {
        Disabled: false,
        Destinations: {
            [destinationIdentifier]: {
                Disabled: false,
                AzureSettings: azureSettings,
                S3Settings: null,
            },
        },
    };
}

describe("remoteAttachmentsUtils azure credentials", () => {
    it("selects account key authentication when DTO has an account key", () => {
        const form = remoteAttachmentsUtils.mapFromDto(
            createConfiguration({ ...azureSettingsBase, AccountKey: "key" })
        );

        expect(form.destinations[0].azure.authType).toBe("accountKey");
        expect(form.destinations[0].azure.accountKey).toBe("key");
    });

    it("selects SAS token authentication when DTO has a SAS token", () => {
        const form = remoteAttachmentsUtils.mapFromDto(
            createConfiguration({ ...azureSettingsBase, SasToken: sasToken })
        );

        expect(form.destinations[0].azure.authType).toBe("sasToken");
        expect(form.destinations[0].azure.sasToken).toBe(sasToken);
    });

    it("keeps SAS token when a destination round-trips through the form", () => {
        const dto = createConfiguration({ ...azureSettingsBase, SasToken: sasToken });

        const roundTripped = remoteAttachmentsUtils.mapToDto(remoteAttachmentsUtils.mapFromDto(dto));

        expect(roundTripped.Destinations[destinationIdentifier].AzureSettings.SasToken).toBe(sasToken);
        expect(roundTripped.Destinations[destinationIdentifier].AzureSettings.AccountKey).toBeNull();
    });

    it("sends only the selected credential to the server", () => {
        const form = remoteAttachmentsUtils.mapFromDto(
            createConfiguration({ ...azureSettingsBase, AccountKey: "key" })
        );
        form.destinations[0].azure.authType = "sasToken";
        form.destinations[0].azure.sasToken = sasToken;

        const azureSettings = remoteAttachmentsUtils.mapToDto(form).Destinations[destinationIdentifier].AzureSettings;

        expect(azureSettings.SasToken).toBe(sasToken);
        expect(azureSettings.AccountKey).toBeNull();
    });
});
