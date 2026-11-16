import { Meta, StoryObj } from "@storybook/react-webpack5";
import { screen, userEvent, within } from "storybook/test";
import {
    databaseAccessArgType,
    databaseArgType,
    DatabaseType,
    withBootstrap5,
    withForceRerender,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { SharedStubs } from "test/stubs/SharedStubs";
import { createTranslator } from "common/i18n/i18n";
import EditBackupTask from "./EditBackupTask";
import { EditBackupTaskStepId } from "./utils/editBackupTaskSteps";

const t = createTranslator("editBackupTask");
const destinationsT = createTranslator("formDestinations");
const commonT = createTranslator("common");

type PeriodicBackupConfiguration = Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration;

type EditBackupTaskMode = "newPeriodic" | "editPeriodic" | "manual";

interface EditBackupTaskStoryArgs {
    mode: EditBackupTaskMode;
    databaseType: DatabaseType;
    databaseAccess: databaseAccessLevel;
    isDatabaseEncrypted: boolean;
    backupType: "Backup" | "Snapshot";
    hasPeriodicBackup: boolean;
    hasSnapshotBackups: boolean;
    hasCloudBackups: boolean;
    hasEncryptedBackups: boolean;
    hasDestinations: boolean;
    disabledDestinations: "none" | "one" | "all";
    isOnlyLocalAllowed: boolean;
    hasLongValues: boolean;
    isLoadError: boolean;
    isDestinationTestFailing: boolean;
}

export default {
    title: "Pages/Tasks/Backups/Edit Backup Task",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/MJ7dtcrwfaTbzxZ9KdETUS/Pages---Backups?node-id=268-2226",
        },
    },
    argTypes: {
        mode: { control: "radio", options: ["newPeriodic", "editPeriodic", "manual"] },
        databaseType: databaseArgType,
        databaseAccess: databaseAccessArgType,
        backupType: { control: "radio", options: ["Backup", "Snapshot"] },
        disabledDestinations: { control: "radio", options: ["none", "one", "all"] },
    },
    args: {
        mode: "editPeriodic",
        databaseType: "cluster",
        databaseAccess: "DatabaseAdmin",
        isDatabaseEncrypted: false,
        backupType: "Backup",
        hasPeriodicBackup: true,
        hasSnapshotBackups: true,
        hasCloudBackups: true,
        hasEncryptedBackups: true,
        hasDestinations: true,
        disabledDestinations: "none",
        isOnlyLocalAllowed: false,
        hasLongValues: false,
        isLoadError: false,
        isDestinationTestFailing: false,
    },
    render: (args) => {
        const { databases, license, cluster, accessManager } = mockStore;
        const { tasksService, manageServerService, databasesService } = mockServices;

        const dbMockValue = (db: { isEncrypted: boolean }) => {
            db.isEncrypted = args.isDatabaseEncrypted;
        };
        const db = (() => {
            if (args.databaseType === "sharded") {
                return databases.withActiveDatabase_Sharded(dbMockValue);
            }
            if (args.databaseType === "cluster") {
                return databases.withActiveDatabase_NonSharded_Cluster(dbMockValue);
            }
            return databases.withActiveDatabase_NonSharded_SingleNode(dbMockValue);
        })();

        accessManager.with_securityClearance("ValidUser");
        accessManager.with_databaseAccess({ [db.name]: args.databaseAccess });
        if (args.databaseType === "singleNode") {
            cluster.with_Single();
        } else {
            cluster.with_Cluster();
        }

        license.with_License({
            HasPeriodicBackup: args.hasPeriodicBackup,
            HasSnapshotBackups: args.hasSnapshotBackups,
            HasCloudBackups: args.hasCloudBackups,
            HasEncryptedBackups: args.hasEncryptedBackups,
        });

        tasksService.withPeriodicBackupServerLimits((limits) => {
            limits.AllowedDestinations = args.isOnlyLocalAllowed ? ["Local"] : null;
        });
        tasksService.withCronExpressionOccurrence();
        tasksService.withBackupLocation();
        tasksService.withLocalFolderPathOptions();
        tasksService.withSavePeriodicBackupConfiguration();
        tasksService.withStartManualBackup();
        manageServerService.withTestPeriodicBackupCredentials(
            args.isDestinationTestFailing ? SharedStubs.nodeConnectionTestErrorResult() : undefined
        );
        databasesService.withGenerateSecret();

        if (args.isLoadError) {
            tasksService.withThrowingPeriodicBackupConfiguration();
        } else {
            tasksService.withPeriodicBackupConfiguration((dto) => applyArgsToDto(dto, args));
        }

        return (
            <div style={{ height: "90vh" }}>
                <EditBackupTask queryParams={getQueryParams(args.mode)} />
            </div>
        );
    },
} satisfies Meta<EditBackupTaskStoryArgs>;

type Story = StoryObj<EditBackupTaskStoryArgs>;

export const Default: Story = {};

export const NewPeriodicBackup: Story = {
    args: { mode: "newPeriodic" },
};

export const NewManualBackup: Story = {
    args: { mode: "manual" },
};

export const Settings: Story = {
    play: async ({ canvasElement }) => goToStep(canvasElement, "settings"),
};

export const SettingsSnapshot: Story = {
    args: { backupType: "Snapshot" },
    play: async ({ canvasElement }) => goToStep(canvasElement, "settings"),
};

export const SettingsEncryptedDatabase: Story = {
    args: { isDatabaseEncrypted: true },
    play: async ({ canvasElement }) => goToStep(canvasElement, "settings"),
};

export const Schedule: Story = {
    play: async ({ canvasElement }) => goToStep(canvasElement, "schedule"),
};

export const Destinations: Story = {
    play: async ({ canvasElement }) => goToStep(canvasElement, "destinations"),
};

export const DestinationsEmpty: Story = {
    args: { hasDestinations: false },
    play: async ({ canvasElement }) => goToStep(canvasElement, "destinations"),
};

export const DestinationsDisabled: Story = {
    args: { disabledDestinations: "one" },
    play: async ({ canvasElement }) => goToStep(canvasElement, "destinations"),
};

export const DestinationsAllDisabled: Story = {
    args: { disabledDestinations: "all" },
    play: async ({ canvasElement }) => {
        await goToStep(canvasElement, "destinations");
        await userEvent.click(await within(canvasElement).findByRole("button", { name: commonT("next") }));
    },
};

export const SummaryDisabledDestination: Story = {
    args: { disabledDestinations: "one" },
};

export const SummaryDestinationTested: Story = {
    play: async ({ canvasElement }) => {
        const testButtons = await within(canvasElement).findAllByRole("button", { name: commonT("test") });
        await userEvent.click(testButtons[0]);
    },
};

export const DestinationEditor: Story = {
    args: { hasDestinations: false },
    play: async ({ canvasElement }) => {
        await goToStep(canvasElement, "destinations");
        await userEvent.click(await within(canvasElement).findByRole("button", { name: t("destinations.add") }));
    },
};

export const DestinationEditorLicenseRestricted: Story = {
    args: { hasDestinations: false, hasCloudBackups: false },
    play: async ({ canvasElement }) => {
        await goToStep(canvasElement, "destinations");
        await userEvent.click(await within(canvasElement).findByRole("button", { name: t("destinations.add") }));
        await userEvent.click(await screen.findByRole("combobox", { name: destinationsT("editor.destination") }));
    },
};

export const DestinationTestFailed: Story = {
    args: { isDestinationTestFailing: true },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await goToStep(canvasElement, "destinations");
        const testButtons = await canvas.findAllByRole("button", { name: destinationsT("testCredentials") });
        await userEvent.click(testButtons[0]);
    },
};

export const DestinationsNoneLicensed: Story = {
    args: { hasCloudBackups: false },
    play: async ({ canvasElement }) => {
        await goToStep(canvasElement, "destinations");
        await userEvent.click(await within(canvasElement).findByRole("button", { name: t("destinations.add") }));
        await userEvent.click(await screen.findByRole("combobox", { name: destinationsT("editor.destination") }));
    },
};

export const DestinationsDisabledByAdministrator: Story = {
    args: { isOnlyLocalAllowed: true },
    play: async ({ canvasElement }) => goToStep(canvasElement, "destinations"),
};

export const Sharded: Story = {
    args: { mode: "newPeriodic", databaseType: "sharded" },
};

export const LicenseRestricted: Story = {
    args: {
        mode: "newPeriodic",
        hasPeriodicBackup: false,
        hasSnapshotBackups: false,
        hasCloudBackups: false,
        hasEncryptedBackups: false,
    },
};

export const LicenseRestrictedExistingTask: Story = {
    args: {
        hasPeriodicBackup: false,
        hasSnapshotBackups: false,
        hasCloudBackups: false,
        hasEncryptedBackups: false,
    },
};

export const LongValues: Story = {
    args: { hasLongValues: true },
};

export const LongValuesDestinations: Story = {
    args: { hasLongValues: true },
    play: async ({ canvasElement }) => goToStep(canvasElement, "destinations"),
};

export const LoadError: Story = {
    args: { isLoadError: true },
};

export const WithoutDatabaseAdminAccess: Story = {
    args: { databaseAccess: "DatabaseReadWrite" },
};

export const NewBackupWithoutDatabaseAdminAccess: Story = {
    args: { mode: "newPeriodic", databaseAccess: "DatabaseReadWrite" },
};

async function goToStep(canvasElement: HTMLElement, stepId: EditBackupTaskStepId) {
    const canvas = within(canvasElement);
    await userEvent.click(await canvas.findByRole("button", { name: t(`steps.${stepId}`) }));
}

function getQueryParams(mode: EditBackupTaskMode) {
    switch (mode) {
        case "newPeriodic":
            return { sourceView: "Backups" as const };
        case "editPeriodic":
            return { sourceView: "Backups" as const, taskId: "1" };
        case "manual":
            return { sourceView: "Backups" as const, manual: "true" };
    }
}

function applyArgsToDto(dto: PeriodicBackupConfiguration, args: EditBackupTaskStoryArgs) {
    dto.BackupType = args.backupType;

    if (args.isDatabaseEncrypted) {
        dto.BackupEncryptionSettings = { EncryptionMode: "UseDatabaseKey", Key: null };
    }

    if (!args.hasDestinations) {
        dto.LocalSettings = null;
        dto.S3Settings = null;
        dto.AzureSettings = null;
    }

    if (args.hasDestinations && args.disabledDestinations !== "none") {
        dto.S3Settings.Disabled = true;
    }

    if (args.hasDestinations && args.disabledDestinations === "all") {
        dto.LocalSettings.Disabled = true;
        dto.AzureSettings.Disabled = true;
    }

    if (args.hasLongValues) {
        dto.Name = "Nightly backup of the production orders database for the European region ".repeat(3);
        dto.LocalSettings.FolderPath =
            "/var/lib/ravendb/backups/production/europe/orders/nightly/full-and-incremental-backups-folder-with-a-very-long-name";
        dto.S3Settings.BucketName = "ravendb-production-europe-orders-nightly-backups-bucket-name-long";
        dto.S3Settings.RemoteFolderName = "production/europe/orders/nightly/remote-folder-with-a-very-long-name";
        dto.AzureSettings.GetBackupConfigurationScript.Arguments =
            "--environment production --region europe-west-1 --vault orders-nightly-backups --verbose --retry 5";
    }
}
