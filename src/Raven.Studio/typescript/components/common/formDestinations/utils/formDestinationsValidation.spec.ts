import { azureSchema } from "./formDestinationsValidation";
import { AzureDestination } from "./formDestinationsTypes";

const enabledAzure: AzureDestination = {
    isEnabled: true,
    config: { isOverrideConfig: false, exec: null, arguments: null, timeoutInMs: null },
    storageContainer: "container",
    remoteFolderName: null,
    accountName: "account",
    accountKey: null,
    sasToken: null,
};

describe("azureSchema credentials", () => {
    it("accepts an account key without a SAS token", async () => {
        await expect(azureSchema.isValid({ ...enabledAzure, accountKey: "key" })).resolves.toBe(true);
    });

    it("accepts a SAS token without an account key", async () => {
        await expect(azureSchema.isValid({ ...enabledAzure, sasToken: "sv=2024-01-01&sig=abc" })).resolves.toBe(true);
    });

    it("rejects when both account key and SAS token are empty", async () => {
        await expect(azureSchema.validate(enabledAzure)).rejects.toThrow("Account key or SAS token is required");
    });

    it("rejects when both account key and SAS token are provided", async () => {
        await expect(azureSchema.validate({ ...enabledAzure, accountKey: "key", sasToken: "sv=1" })).rejects.toThrow(
            "Account key and SAS token cannot be used simultaneously"
        );
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
