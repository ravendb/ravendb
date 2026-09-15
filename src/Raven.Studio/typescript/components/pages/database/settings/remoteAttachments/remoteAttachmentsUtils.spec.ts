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
    it("maps account key from DTO into the form", () => {
        const form = remoteAttachmentsUtils.mapFromDto(
            createConfiguration({ ...azureSettingsBase, AccountKey: "key" })
        );

        expect(form.destinations[0].azure.accountKey).toBe("key");
        expect(form.destinations[0].azure.sasToken).toBeNull();
    });

    it("maps SAS token from DTO into the form", () => {
        const form = remoteAttachmentsUtils.mapFromDto(
            createConfiguration({ ...azureSettingsBase, SasToken: sasToken })
        );

        expect(form.destinations[0].azure.sasToken).toBe(sasToken);
        expect(form.destinations[0].azure.accountKey).toBeNull();
    });

    it("keeps SAS token when a destination round-trips through the form", () => {
        const dto = createConfiguration({ ...azureSettingsBase, SasToken: sasToken });

        const roundTripped = remoteAttachmentsUtils.mapToDto(remoteAttachmentsUtils.mapFromDto(dto));

        expect(roundTripped.Destinations[destinationIdentifier].AzureSettings.SasToken).toBe(sasToken);
        expect(roundTripped.Destinations[destinationIdentifier].AzureSettings.AccountKey).toBeNull();
    });
});
