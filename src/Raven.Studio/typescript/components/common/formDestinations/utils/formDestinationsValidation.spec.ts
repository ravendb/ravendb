import { azureSchema } from "./formDestinationsValidation";
import { AzureDestination } from "./formDestinationsTypes";

const enabledAzure: AzureDestination = {
    isEnabled: true,
    config: { isOverrideConfig: false, exec: null, arguments: null, timeoutInMs: null },
    storageContainer: "container",
    remoteFolderName: null,
    accountName: "account",
    authType: "accountKey",
    accountKey: null,
    sasToken: null,
};

const sasToken = "sv=2024-01-01&sig=abc";

describe("azureSchema credentials", () => {
    it("requires an account key when account key authentication is selected", async () => {
        await expect(azureSchema.isValid(enabledAzure)).resolves.toBe(false);
        await expect(azureSchema.isValid({ ...enabledAzure, accountKey: "key" })).resolves.toBe(true);
    });

    it("requires a SAS token when SAS token authentication is selected", async () => {
        const sasTokenAzure: AzureDestination = { ...enabledAzure, authType: "sasToken" };

        await expect(azureSchema.isValid(sasTokenAzure)).resolves.toBe(false);
        await expect(azureSchema.isValid({ ...sasTokenAzure, sasToken })).resolves.toBe(true);
    });

    it("ignores the credential that is not selected", async () => {
        await expect(azureSchema.isValid({ ...enabledAzure, accountKey: "key", sasToken })).resolves.toBe(true);
        await expect(
            azureSchema.isValid({ ...enabledAzure, authType: "sasToken", accountKey: "key", sasToken })
        ).resolves.toBe(true);
    });

    it("skips credential validation when the destination is disabled", async () => {
        await expect(azureSchema.isValid({ ...enabledAzure, isEnabled: false })).resolves.toBe(true);
    });

    it("skips credential validation when configuration is overridden by script", async () => {
        const overridden: AzureDestination = {
            ...enabledAzure,
            config: { isOverrideConfig: true, exec: "script.sh", arguments: "--arg", timeoutInMs: 1000 },
        };

        await expect(azureSchema.isValid(overridden)).resolves.toBe(true);
    });
});
