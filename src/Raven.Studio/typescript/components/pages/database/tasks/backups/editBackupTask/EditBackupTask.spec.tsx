import { useState } from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender, rtlRender_WithWaitForLoad, commonSelectors, within, waitFor, fireEvent } from "test/rtlTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import notificationCenter from "common/notifications/notificationCenter";
import EditBackupTask from "./EditBackupTask";
import * as stories from "./EditBackupTask.stories";

const {
    Default,
    NewPeriodicBackup,
    NewManualBackup,
    Sharded,
    LicenseRestricted,
    LicenseRestrictedExistingTask,
    DestinationEditorLicenseRestricted,
    LoadError,
    DestinationsEmpty,
    DestinationsNoneLicensed,
    DestinationsDisabledByAdministrator,
    DestinationTestFailed,
    WithoutDatabaseAdminAccess,
    NewBackupWithoutDatabaseAdminAccess,
} = composeStories(stories);

describe("EditBackupTask", () => {
    it("opens the next step after the user selects a backup type", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<NewPeriodicBackup />);

        const nextButton = screen.getByRole("button", { name: "Next" });
        expect(nextButton).toBeDisabled();

        await fireClick(screen.getByRole("radio", { name: "Backup" }));
        await fireClick(nextButton);

        expect(await screen.findByRole("heading", { level: 2, name: /Configure settings/ })).toBeInTheDocument();
        expect(screen.getByName("name")).toBeInTheDocument();
    });

    it("does not show the schedule step for a manual backup", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<NewManualBackup />);

        expect(screen.getByText("Define destinations")).toBeInTheDocument();
        expect(screen.queryByText("Schedule backups")).not.toBeInTheDocument();
    });

    it("disables the snapshot type for a sharded database", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<Sharded />);

        expect(screen.getByRole("radio", { name: "Snapshot" })).toHaveAttribute("aria-disabled", "true");
        expect(screen.getByRole("radio", { name: "Backup" })).toHaveAttribute("aria-checked", "true");
    });

    it("blocks a periodic backup when the license does not include it", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<LicenseRestricted />);

        expect(screen.getByText(/doesn't include periodic backups/)).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
    });

    it("blocks a new backup when the user has no database admin access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<NewBackupWithoutDatabaseAdminAccess />);

        expect(screen.getByText("You need Database Admin access to create a backup.")).toBeInTheDocument();
        expect(screen.getByRole("radio", { name: "Backup" })).toHaveAttribute("aria-disabled", "true");
        expect(screen.getByRole("button", { name: "Next" })).toBeDisabled();
    });

    it("shows the error details after a failed destination test", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<DestinationTestFailed />);

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));
        const s3Panel = screen.getByLabelText("S3 Compatible");
        await fireClick(within(s3Panel).getByRole("button", { name: "Test credentials" }));

        const errorDialog = await screen.findByRole("dialog");
        expect(within(errorDialog).getByText("Connection test failed")).toBeInTheDocument();

        await fireClick(within(errorDialog).getByText("Close"));
        await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
        expect(within(s3Panel).getByRole("button", { name: "Failed connection" })).toBeInTheDocument();
    });

    it("hides the destination test result after the user edits the destination", async () => {
        const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<DestinationTestFailed />);

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));
        const s3Panel = screen.getByLabelText("S3 Compatible");
        await fireClick(within(s3Panel).getByRole("button", { name: "Test credentials" }));
        await fireClick(within(await screen.findByRole("dialog")).getByText("Close"));
        await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
        expect(within(s3Panel).getByText("Failed connection")).toBeInTheDocument();

        await fireClick(screen.getByTitle("Edit the S3 Compatible destination"));
        await fillInput(screen.getByName("destinations.s3.awsSecretKey"), "new-secret-key");
        await fireClick(screen.getByRole("button", { name: "Apply configuration" }));

        expect(await within(s3Panel).findByRole("button", { name: "Test credentials" })).toBeInTheDocument();
        expect(within(s3Panel).queryByText("Failed connection")).not.toBeInTheDocument();
    });

    it("hides the destination test result after the user edits a field in the destination editor", async () => {
        const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<DestinationTestFailed />);

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));
        await fireClick(screen.getByTitle("Edit the S3 Compatible destination"));
        const secretKeyInput = screen.getByName("destinations.s3.awsSecretKey");
        const editor = within(secretKeyInput.closest("form"));

        await fireClick(editor.getByRole("button", { name: "Test credentials" }));
        await fireClick(within(await screen.findByRole("dialog")).getByText("Close"));
        await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
        expect(editor.getByRole("button", { name: "Failed connection" })).toBeInTheDocument();

        await fillInput(secretKeyInput, "new-secret-key");

        expect(await editor.findByRole("button", { name: "Test credentials" })).toBeInTheDocument();
        expect(editor.queryByText("Failed connection")).not.toBeInTheDocument();
    });

    it("saves an edited task", async () => {
        const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<Default />);
        const saveMock = mockServices.tasksService.getMock("savePeriodicBackupConfiguration");

        const settingsSection = screen.getByRole("region", { name: "Configuration settings" });
        await fireClick(within(settingsSection).getByRole("button", { name: /Edit/ }));
        await fillInput(screen.getByName("name"), "Weekly backup");

        await fireClick(screen.getByRole("button", { name: "Next" }));
        await fireClick(await screen.findByRole("button", { name: "Next" }));
        await fireClick(await screen.findByRole("button", { name: "Next" }));
        await fireClick(await screen.findByRole("button", { name: "Save" }));

        expect(saveMock).toHaveBeenCalledWith(
            expect.any(String),
            expect.objectContaining({ TaskId: 1, Name: "Weekly backup", BackupType: "Backup" })
        );
    });

    it("clears a validation error after the user fixes the field", async () => {
        const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<Default />);

        const settingsSection = screen.getByRole("region", { name: "Configuration settings" });
        await fireClick(within(settingsSection).getByRole("button", { name: /Edit/ }));

        const maxReadOpsInput = screen.getByName("maxReadOpsPerSecond");
        await fillInput(maxReadOpsInput, "");
        await fireClick(screen.getByRole("button", { name: "Next" }));
        expect(maxReadOpsInput).toHaveClass("is-invalid");

        await fillInput(maxReadOpsInput, "500");
        await waitFor(() => expect(maxReadOpsInput).not.toHaveClass("is-invalid"));
    });

    it("adds a destination after the user changes the destination type in the editor", async () => {
        const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<DestinationsEmpty />);
        const noDestinationError = "Please select at least one destination";

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));
        await fireClick(screen.getByRole("button", { name: "Next" }));
        expect(await screen.findByText(noDestinationError)).toBeInTheDocument();

        await fireClick(screen.getByRole("button", { name: "Add destination" }));
        await selectDestinationType("Azure");
        await fireClick(screen.getByRole("radio", { name: "External script" }));
        await selectDestinationType("Local");
        await fireClick(screen.getByRole("radio", { name: "External script" }));
        await fillInput(screen.getByName("destinations.local.config.exec"), "/opt/scripts/get-backup-path.sh");
        await fillInput(screen.getByName("destinations.local.config.arguments"), "--verbose");
        await fillInput(screen.getByName("destinations.local.config.timeoutInMs"), "10000");
        await fireClick(screen.getByRole("button", { name: "Apply configuration" }));

        expect(await screen.findByTitle("Remove the Local destination")).toBeInTheDocument();
        await waitFor(() => expect(screen.queryByText(noDestinationError)).not.toBeInTheDocument());

        async function selectDestinationType(label: string) {
            fireEvent.keyDown(screen.getByRole("combobox", { name: "Destination" }), { key: "ArrowDown" });
            await fireClick(await screen.findByText(label));
        }
    });

    it("shows the chosen destination when the editor is opened again for another destination", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<Default />);

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));
        await fireClick(screen.getByTitle("Edit the S3 Compatible destination"));
        expect(screen.getByName("destinations.s3.bucketName")).toBeInTheDocument();

        await fireClick(screen.getByTitle("Edit the Local destination"));
        expect(screen.queryByName("destinations.s3.bucketName")).not.toBeInTheDocument();
        expect(screen.getByName("destinations.local.folderPath")).toBeInTheDocument();
    });

    it("closes the destination editor when the user leaves the destinations step", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<Default />);

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));
        await fireClick(screen.getByTitle("Edit the S3 Compatible destination"));
        await fireClick(screen.getByRole("button", { name: /Back/ }));

        expect(screen.queryByName("destinations.s3.bucketName")).not.toBeInTheDocument();
    });

    it("asks before it drops unsaved destination changes when the user leaves the destinations step", async () => {
        const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<Default />);

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));
        await fireClick(screen.getByTitle("Edit the S3 Compatible destination"));
        await fillInput(screen.getByName("destinations.s3.bucketName"), "other-bucket");
        await fireClick(screen.getByRole("button", { name: /Back/ }));

        expect(await screen.findByText("You have unsaved changes. Do you want to discard them?")).toBeInTheDocument();
        expect(screen.getByName("destinations.s3.bucketName")).toHaveValue("other-bucket");

        await fireClick(screen.getByRole("button", { name: "Discard" }));
        await waitFor(() => expect(screen.queryByName("destinations.s3.bucketName")).not.toBeInTheDocument());
        expect(screen.getByRole("heading", { level: 2, name: /Schedule your backup/ })).toBeInTheDocument();
    });

    it("shows the remaining destinations when the license covers none of them", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<DestinationsNoneLicensed />);

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));
        await fireClick(screen.getByRole("button", { name: "Add destination" }));
        fireEvent.keyDown(screen.getByRole("combobox", { name: "Destination" }), { key: "ArrowDown" });

        const options = await screen.findAllByRole("option");
        expect(options).toHaveLength(3);
        options.forEach((option) => {
            expect(option).toHaveAttribute("aria-disabled", "true");
            expect(within(option).getByTestId("license-restricted-badge")).toBeInTheDocument();
        });
        expect(screen.getByRole("button", { name: "Apply configuration" })).toBeDisabled();
    });

    it("does not let the user add a destination when the administrator disabled the remaining ones", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<DestinationsDisabledByAdministrator />);

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));

        expect(screen.getByRole("button", { name: "Add destination" })).toBeDisabled();
    });

    it("shows the required license on the destinations that the license does not include", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<DestinationEditorLicenseRestricted />);

        await fireClick(screen.getByRole("button", { name: "Define destinations" }));
        await fireClick(screen.getByRole("button", { name: "Add destination" }));
        fireEvent.keyDown(screen.getByRole("combobox", { name: "Destination" }), { key: "ArrowDown" });

        const azureOption = await screen.findByRole("option", { name: /Azure/ });
        expect(azureOption).toHaveAttribute("aria-disabled", "true");
        expect(within(azureOption).getByTestId("license-restricted-badge")).toBeInTheDocument();

        const localOption = screen.getByRole("option", { name: /Local/ });
        expect(localOption).toHaveAttribute("aria-disabled", "false");
        expect(within(localOption).queryByTestId("license-restricted-badge")).not.toBeInTheDocument();
    });

    it("lists the license dependent backup features in the license comparison", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<LicenseRestricted />);

        const licenseComparison = await screen.findByRole("dialog");
        expect(within(licenseComparison).getByText("Periodic Backups")).toBeInTheDocument();
        expect(within(licenseComparison).getByText("Cloud & Remote Destinations")).toBeInTheDocument();
        expect(within(licenseComparison).getByText("Encrypted Backups")).toBeInTheDocument();
        expect(within(licenseComparison).getByText("Snapshot Backups")).toBeInTheDocument();
    });

    it("does not open the license comparison again after the user goes to another step", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<LicenseRestrictedExistingTask />);

        const licenseComparison = await screen.findByRole("dialog");
        await fireClick(within(licenseComparison).getByText("Close"));
        expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

        await fireClick(screen.getByRole("button", { name: "Configure settings" }));

        expect(await screen.findByRole("heading", { level: 2, name: /Configure settings/ })).toBeInTheDocument();
        expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    });

    it("clears the schedule error after the user enables the full backup", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<NewPeriodicBackup />);
        const noScheduleError = "Enable a full or an incremental backup to continue";

        await fireClick(screen.getByRole("radio", { name: "Backup" }));
        await fireClick(screen.getByRole("button", { name: "Next" }));
        await fireClick(await screen.findByRole("button", { name: "Next" }));
        await screen.findByRole("heading", { level: 2, name: /Schedule your backup/ });
        await fireClick(screen.getByRole("button", { name: "Next" }));
        expect(await screen.findByText(noScheduleError)).toBeInTheDocument();

        await fireClick(screen.getByRole("checkbox", { name: "Enable full backup" }));
        await waitFor(() => expect(screen.queryByText(noScheduleError)).not.toBeInTheDocument());
    });

    it("does not let a user without database admin access change the task", async () => {
        const getServerLimitsMock = mockServices.tasksService.getMock("getPeriodicBackupServerLimits");
        getServerLimitsMock.mockClear();

        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<WithoutDatabaseAdminAccess />);

        expect(getServerLimitsMock).not.toHaveBeenCalled();
        expect(screen.getByText("You need Database Admin access to change this backup task.")).toBeInTheDocument();

        const settingsSection = screen.getByRole("region", { name: "Configuration settings" });
        await fireClick(within(settingsSection).getByRole("button", { name: /Edit/ }));
        expect(screen.getByName("name")).toBeDisabled();
        expect(screen.getByRole("checkbox", { name: /Set max read operations/ })).toBeDisabled();

        await fireClick(screen.getByRole("button", { name: "Next" }));
        expect(await screen.findByRole("checkbox", { name: "Enable full backup" })).toBeDisabled();

        await fireClick(screen.getByRole("button", { name: "Next" }));
        expect(await screen.findByRole("button", { name: "Add destination" })).toBeDisabled();
        expect(screen.getByTitle("Remove the Local destination")).toBeDisabled();

        await fireClick(screen.getByRole("button", { name: "Next" }));
        expect(await screen.findByRole("button", { name: "Save" })).toBeDisabled();
    });

    it("shows an error when the task cannot be loaded", async () => {
        const { screen } = rtlRender(<LoadError />);

        expect(await screen.findByText(commonSelectors.loadingError)).toBeInTheDocument();
    });
});

type PeriodicBackupConfiguration = Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration;
type BackupConfiguration = Raven.Client.Documents.Operations.Backups.BackupConfiguration;
type BackupType = Raven.Client.Documents.Operations.Backups.BackupType;

const generatedEncryptionKey = "MXEv4ntxod7qM4mOeF9YZlKIuar1RKU8yQcQSESCzys=";
const originalEncryptionKey = "b3JpZ2luYWwtYmFja3VwLWVuY3J5cHRpb24ta2V5ISE=";
const googleCredentialsJson = '{ "type": "service_account", "private_key": "key", "client_email": "a@b.com" }';
const backupScript = { Exec: "/opt/scripts/get-credentials.sh", Arguments: "--env production", TimeoutInMs: 5000 };

describe("EditBackupTask saved configuration", () => {
    beforeEach(() => {
        mockServices.tasksService.getMock("savePeriodicBackupConfiguration").mockClear();
        mockServices.manageServerService.getMock("testPeriodicBackupCredentials").mockClear();
        mockServices.tasksService.getMock("startManualBackup").mockClear();
        jest.spyOn(notificationCenter.instance, "openDetailsForOperationById").mockImplementation(() => null);
    });

    describe("new periodic task", () => {
        it("sends the default settings", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps();

            expect(await getSavedPeriodicDto()).toEqual(newPeriodicDto());
        });

        it("sends the task name and the disabled state", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                settings: async () => {
                    await wizard.fill("name", "Nightly backup");
                    await wizard.selectByLabel("Task state", "Disabled");
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(newPeriodicDto({ Name: "Nightly backup", Disabled: true }));
        });

        it("sends the direct upload mode", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                settings: () => wizard.selectByLabel("Backup upload mode", "Direct Upload"),
            });

            expect(await getSavedPeriodicDto()).toEqual(newPeriodicDto({ BackupUploadMode: "DirectUpload" }));
        });

        it("sends a null compression algorithm when the snapshot uses the default algorithm", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                backupType: "Snapshot",
                settings: async () => {
                    await wizard.selectByLabel("Compression algorithm", "Default");
                    await wizard.selectByLabel("Compression level", "Optimal");
                    await wizard.toggle("Exclude indexes data");
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    BackupType: "Snapshot",
                    SnapshotSettings: { CompressionAlgorithm: null, CompressionLevel: "Optimal", ExcludeIndexes: true },
                })
            );
        });

        it("sends the chosen snapshot compression algorithm and level", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                backupType: "Snapshot",
                settings: async () => {
                    await wizard.selectByLabel("Compression algorithm", "Deflate");
                    await wizard.selectByLabel("Compression level", "Smallest Size");
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    BackupType: "Snapshot",
                    SnapshotSettings: {
                        CompressionAlgorithm: "Deflate",
                        CompressionLevel: "SmallestSize",
                        ExcludeIndexes: false,
                    },
                })
            );
        });

        it("sends only the incremental schedule", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                schedule: () => wizard.toggle("Enable incremental backup"),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({ FullBackupFrequency: null, IncrementalBackupFrequency: "0 2 * * *" })
            );
        });

        it("sends both schedules", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                schedule: async () => {
                    await wizard.toggle("Enable full backup");
                    await wizard.toggle("Enable incremental backup");
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({ FullBackupFrequency: "0 2 * * 0", IncrementalBackupFrequency: "0 2 * * *" })
            );
        });

        it("sends a custom cron expression", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                schedule: async () => {
                    await wizard.toggle("Enable full backup");
                    await wizard.fireClick(wizard.screen.getAllByRole("radio", { name: "Advanced" })[0]);
                    await wizard.fill("fullBackupFrequency", "0 3 * * 1");
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(newPeriodicDto({ FullBackupFrequency: "0 3 * * 1" }));
        });

        it("does not leave the schedule step when the server rejects the cron expression", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic", cronServerError: "Rejected by the server" });

            await wizard.fireClick(wizard.screen.getByRole("radio", { name: "Backup" }));
            await wizard.fireClick(wizard.screen.getByRole("button", { name: "Next" }));
            await wizard.fireClick(await wizard.screen.findByRole("button", { name: "Next" }));
            await wizard.toggle("Enable full backup");
            await wizard.fireClick(wizard.screen.getAllByRole("radio", { name: "Advanced" })[0]);

            expect(await wizard.screen.findAllByText("Rejected by the server")).not.toHaveLength(0);
            await wizard.fireClick(wizard.screen.getByRole("button", { name: "Next" }));

            expect(wizard.screen.getByRole("heading", { level: 2 })).toHaveTextContent("Schedule your backup");
        });

        it("sends the retention policy", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                settings: async () => {
                    await wizard.toggle("Limit # of backups by age");
                    await wizard.fillInput(
                        await wizard.screen.findByRole("combobox", { name: "Minimum retention time" }),
                        "3d"
                    );
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({ RetentionPolicy: { Disabled: false, MinimumBackupAgeToKeep: "3.00:00:00" } })
            );
        });

        it("sends the max read operations per second", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                settings: async () => {
                    await wizard.toggle("Set max read operations per second");
                    await wizard.fill("maxReadOpsPerSecond", "500");
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(newPeriodicDto({ MaxReadOpsPerSecond: 500 }));
        });

        it("sends the responsible node without the pin", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                settings: () => wizard.selectResponsibleNode("Node B"),
            });

            expect(await getSavedPeriodicDto()).toEqual(newPeriodicDto({ MentorNode: "B", PinToMentorNode: false }));
        });

        it("offers only the nodes of the database group as the responsible node", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic", databaseGroupNodeTags: ["A", "B"] });

            await wizard.fireClick(wizard.screen.getByRole("radio", { name: "Backup" }));
            await wizard.fireClick(wizard.screen.getByRole("button", { name: "Next" }));

            const toggleSwitch = await wizard.screen.findByRole("checkbox", { name: "Set Responsible Node" });
            await wizard.fireClick(toggleSwitch);
            const settingRow = toggleSwitch.closest<HTMLElement>(".edit-backup-task-settings-row");
            fireEvent.keyDown(within(settingRow).getByRole("combobox"), { key: "ArrowDown" });

            expect(await wizard.screen.findByRole("option", { name: "Node B" })).toBeInTheDocument();
            expect(wizard.screen.queryByRole("option", { name: "Node C" })).not.toBeInTheDocument();
        });

        it("sends the pinned responsible node", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                settings: async () => {
                    await wizard.selectResponsibleNode("Node B");
                    await wizard.toggle("Pin node");
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(newPeriodicDto({ MentorNode: "B", PinToMentorNode: true }));
        });

        it("does not send the pin after the user turns off the responsible node", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                settings: async () => {
                    await wizard.selectResponsibleNode("Node B");
                    await wizard.toggle("Pin node");
                    await wizard.toggle("Set Responsible Node");
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({ MentorNode: undefined, PinToMentorNode: false })
            );
        });

        it("sends a relative local path prefixed with the root path set by the administrator", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic", localRootPath: "/srv/backups/" });

            await wizard.completeAllSteps({
                destinations: () => wizard.addLocalDestination("orders"),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({ LocalSettings: localDto({ FolderPath: "/srv/backups/orders" }) })
            );
        });

        it("sends the backup type without a choice for a sharded database", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic", isSharded: true });

            await wizard.completeAllSteps({
                backupType: null,
                settings: async () => {
                    expect(wizard.screen.getByRole("checkbox", { name: "Set Responsible Node" })).toBeDisabled();
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(newPeriodicDto({ BackupType: "Backup" }));
        });
    });

    describe("encryption", () => {
        it("sends a provided key when the user encrypts a backup of a database that is not encrypted", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                settings: async () => {
                    await wizard.toggle("Encrypt your backup");
                    await wizard.confirmEncryptionKeySaved();
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    BackupEncryptionSettings: { EncryptionMode: "UseProvidedKey", Key: generatedEncryptionKey },
                })
            );
        });

        it("sends no encryption for a snapshot of a database that is not encrypted", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                backupType: "Snapshot",
                settings: async () => {
                    expect(wizard.screen.getByRole("checkbox", { name: "Encrypt your backup" })).toBeDisabled();
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    BackupType: "Snapshot",
                    BackupEncryptionSettings: { EncryptionMode: "None", Key: undefined },
                })
            );
        });

        it("sends the database key by default for an encrypted database", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic", isDatabaseEncrypted: true });

            await wizard.completeAllSteps();

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({ BackupEncryptionSettings: { EncryptionMode: "UseDatabaseKey", Key: undefined } })
            );
        });

        it("sends a provided key for an encrypted database", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic", isDatabaseEncrypted: true });

            await wizard.completeAllSteps({
                settings: async () => {
                    await wizard.selectOption(
                        wizard.screen.getByRole("combobox", { name: "Authentication method" }),
                        "Provide your own encryption key"
                    );
                    await wizard.confirmEncryptionKeySaved();
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    BackupEncryptionSettings: { EncryptionMode: "UseProvidedKey", Key: generatedEncryptionKey },
                })
            );
        });

        it("sends no encryption for an encrypted database only after the user confirms it", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic", isDatabaseEncrypted: true });

            await wizard.completeAllSteps({
                settings: async () => {
                    await wizard.toggle("Encrypt your backup");
                    await wizard.fireClick(wizard.screen.getByRole("button", { name: "Next" }));
                    expect(
                        await wizard.screen.findByText(
                            "Confirm that you want an unencrypted backup of an encrypted database"
                        )
                    ).toBeInTheDocument();

                    await wizard.toggle("Allow unencrypted backup for encrypted database");
                },
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({ BackupEncryptionSettings: { EncryptionMode: "None", Key: undefined } })
            );
        });

        it("sends the database key for a snapshot of an encrypted database", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic", isDatabaseEncrypted: true });

            await wizard.completeAllSteps({ backupType: "Snapshot" });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    BackupType: "Snapshot",
                    BackupEncryptionSettings: { EncryptionMode: "UseDatabaseKey", Key: undefined },
                })
            );
        });
    });

    describe("destinations", () => {
        it("sends an Amazon S3 destination", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("S3 Compatible", async () => {
                        await wizard.selectByLabel("Region", "Europe (Ireland) - eu-west-1");
                        await wizard.fill("destinations.s3.awsAccessKey", "access-key");
                        await wizard.fill("destinations.s3.awsSecretKey", "secret-key");
                        await wizard.fill("destinations.s3.bucketName", "ravendb-backups");
                        await wizard.fill("destinations.s3.remoteFolderName", "nightly");
                        await wizard.selectByLabel("Storage class", "Intelligent Tiering");
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    S3Settings: {
                        Disabled: false,
                        GetBackupConfigurationScript: undefined,
                        AwsRegionName: "eu-west-1",
                        AwsAccessKey: "access-key",
                        AwsSecretKey: "secret-key",
                        AwsSessionToken: null,
                        RemoteFolderName: "nightly",
                        BucketName: "ravendb-backups",
                        CustomServerUrl: undefined,
                        ForcePathStyle: undefined,
                        DisableChecksumValidation: undefined,
                        StorageClass: "IntelligentTiering",
                    },
                })
            );
        });

        it("sends an S3 destination with a custom server URL and region", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("S3 Compatible", async () => {
                        await wizard.selectByLabel("Provider", "Custom");
                        await wizard.fill("destinations.s3.customServerUrl", "https://minio.example.com:9000");
                        await wizard.fill("destinations.s3.awsRegionName", "custom-region-1");
                        await wizard.toggle(/Force path style/);
                        await wizard.toggle(/Disable checksum validation/);
                        await wizard.fill("destinations.s3.awsAccessKey", "access-key");
                        await wizard.fill("destinations.s3.awsSecretKey", "secret-key");
                        await wizard.fill("destinations.s3.bucketName", "ravendb-backups");
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    S3Settings: {
                        Disabled: false,
                        GetBackupConfigurationScript: undefined,
                        AwsRegionName: "custom-region-1",
                        AwsAccessKey: "access-key",
                        AwsSecretKey: "secret-key",
                        AwsSessionToken: null,
                        RemoteFolderName: null,
                        BucketName: "ravendb-backups",
                        CustomServerUrl: "https://minio.example.com:9000",
                        ForcePathStyle: true,
                        DisableChecksumValidation: true,
                        StorageClass: "Standard",
                    },
                })
            );
        });

        it("drops the custom server URL when the S3 destination uses an external script", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("S3 Compatible", async () => {
                        await wizard.selectByLabel("Provider", "Custom");
                        await wizard.fill("destinations.s3.customServerUrl", "https://minio.example.com:9000");
                        await wizard.toggle(/Force path style/);
                        await wizard.useExternalScript("s3");
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    S3Settings: {
                        Disabled: false,
                        GetBackupConfigurationScript: backupScript,
                        AwsRegionName: null,
                        AwsAccessKey: null,
                        AwsSecretKey: null,
                        AwsSessionToken: null,
                        RemoteFolderName: null,
                        BucketName: null,
                        CustomServerUrl: undefined,
                        ForcePathStyle: undefined,
                        DisableChecksumValidation: undefined,
                        StorageClass: "Standard",
                    },
                })
            );
        });

        it("sends an Azure destination with an account key", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("Azure", async () => {
                        await wizard.fill("destinations.azure.accountName", "ravendbaccount");
                        await wizard.fill("destinations.azure.accountKey", "account-key");
                        await wizard.fill("destinations.azure.storageContainer", "backups");
                        await wizard.fill("destinations.azure.remoteFolderName", "nightly");
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    AzureSettings: {
                        Disabled: false,
                        GetBackupConfigurationScript: undefined,
                        AccountName: "ravendbaccount",
                        AccountKey: "account-key",
                        SasToken: null,
                        StorageContainer: "backups",
                        RemoteFolderName: "nightly",
                    },
                })
            );
        });

        it("sends an Azure destination with a SAS token and without the account key", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("Azure", async () => {
                        await wizard.fill("destinations.azure.accountName", "ravendbaccount");
                        await wizard.fill("destinations.azure.accountKey", "account-key");
                        await wizard.fireClick(wizard.screen.getByLabelText("SAS token"));
                        await wizard.fill("destinations.azure.sasToken", " sas-token ");
                        await wizard.fill("destinations.azure.storageContainer", "backups");
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    AzureSettings: {
                        Disabled: false,
                        GetBackupConfigurationScript: undefined,
                        AccountName: "ravendbaccount",
                        AccountKey: null,
                        SasToken: "sas-token",
                        StorageContainer: "backups",
                        RemoteFolderName: null,
                    },
                })
            );
        });

        it("sends a Google Cloud destination", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("Google Cloud", async () => {
                        await wizard.fill("destinations.googleCloud.googleCredentialsJson", googleCredentialsJson);
                        await wizard.fill("destinations.googleCloud.bucketName", "ravendb-backups");
                        await wizard.fill("destinations.googleCloud.remoteFolderName", "nightly");
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    GoogleCloudSettings: {
                        Disabled: false,
                        GetBackupConfigurationScript: undefined,
                        BucketName: "ravendb-backups",
                        RemoteFolderName: "nightly",
                        GoogleCredentialsJson: googleCredentialsJson,
                    },
                })
            );
        });

        it("sends an Amazon Glacier destination", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("Amazon Glacier", async () => {
                        await wizard.selectByLabel("Region", "Europe (Ireland) - eu-west-1");
                        await wizard.fill("destinations.glacier.awsAccessKey", "access-key");
                        await wizard.fill("destinations.glacier.awsSecretKey", "secret-key");
                        await wizard.fill("destinations.glacier.vaultName", "backups-vault");
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    GlacierSettings: {
                        Disabled: false,
                        GetBackupConfigurationScript: undefined,
                        AwsRegionName: "eu-west-1",
                        AwsAccessKey: "access-key",
                        AwsSecretKey: "secret-key",
                        AwsSessionToken: null,
                        RemoteFolderName: null,
                        VaultName: "backups-vault",
                    },
                })
            );
        });

        it("sends an FTP destination without a certificate", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("FTP", async () => {
                        await wizard.fill("destinations.ftp.url", "ftp://ftp.example.com/backups");
                        await wizard.fill("destinations.ftp.userName", "ftp-user");
                        await wizard.fill("destinations.ftp.password", "ftp-password");
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    FtpSettings: {
                        Disabled: false,
                        GetBackupConfigurationScript: undefined,
                        Url: "ftp://ftp.example.com/backups",
                        UserName: "ftp-user",
                        Password: "ftp-password",
                        CertificateAsBase64: null,
                    },
                })
            );
        });

        it("sends an FTPS destination with a certificate", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("FTP", async () => {
                        await wizard.fill("destinations.ftp.url", "ftps://ftp.example.com/backups");
                        await wizard.fill("destinations.ftp.userName", "ftp-user");
                        await wizard.fill("destinations.ftp.password", "ftp-password");
                        await wizard.uploadFtpCertificate("certificate");
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    FtpSettings: {
                        Disabled: false,
                        GetBackupConfigurationScript: undefined,
                        Url: "ftps://ftp.example.com/backups",
                        UserName: "ftp-user",
                        Password: "ftp-password",
                        CertificateAsBase64: "Y2VydGlmaWNhdGU=",
                    },
                })
            );
        });

        it("sends an FTP destination that uses an external script", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () => wizard.addDestination("FTP", () => wizard.useExternalScript("ftp")),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: undefined,
                    FtpSettings: {
                        Disabled: false,
                        GetBackupConfigurationScript: backupScript,
                        Url: null,
                        UserName: null,
                        Password: null,
                        CertificateAsBase64: null,
                    },
                })
            );
        });

        it("sends a local destination that uses an external script", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () => wizard.addDestination("Local", () => wizard.useExternalScript("local")),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: { Disabled: false, GetBackupConfigurationScript: backupScript, FolderPath: null },
                })
            );
        });

        it("leaves the script timeout out of the request when it is empty, so the server uses its default", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("Local", async () => {
                        await wizard.fireClick(wizard.screen.getByRole("radio", { name: "External script" }));
                        await wizard.fill("destinations.local.config.exec", backupScript.Exec);
                    }),
            });

            expect(await getSavedPeriodicDto()).toEqual(
                newPeriodicDto({
                    LocalSettings: {
                        Disabled: false,
                        GetBackupConfigurationScript: {
                            Exec: backupScript.Exec,
                            Arguments: null,
                            TimeoutInMs: undefined,
                        },
                        FolderPath: null,
                    },
                })
            );
        });

        it("sends every destination the user added", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: async () => {
                    await wizard.addLocalDestination("/var/backups");
                    await wizard.addDestination("FTP", () => wizard.useExternalScript("ftp"));
                },
            });

            const dto = await getSavedPeriodicDto();
            expect(dto.LocalSettings).toEqual(localDto());
            expect(dto.FtpSettings.GetBackupConfigurationScript).toEqual(backupScript);
        });

        it("trims the local path and the S3 values", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: async () => {
                    await wizard.addLocalDestination("  /var/backups  ");
                    await wizard.addDestination("S3 Compatible", async () => {
                        await wizard.selectByLabel("Provider", "Custom");
                        await wizard.fill("destinations.s3.customServerUrl", "https://minio.example.com:9000");
                        await wizard.fill("destinations.s3.awsRegionName", " custom-region-1 ");
                        await wizard.fill("destinations.s3.awsAccessKey", " access-key ");
                        await wizard.fill("destinations.s3.awsSecretKey", " secret-key ");
                        await wizard.fill("destinations.s3.bucketName", " ravendb-backups ");
                        await wizard.fill("destinations.s3.remoteFolderName", " nightly ");
                    });
                },
            });

            const dto = await getSavedPeriodicDto();
            expect(dto.LocalSettings).toEqual(localDto({ FolderPath: "/var/backups" }));
            expect(dto.S3Settings).toEqual({
                Disabled: false,
                GetBackupConfigurationScript: undefined,
                AwsRegionName: "custom-region-1",
                AwsAccessKey: "access-key",
                AwsSecretKey: "secret-key",
                AwsSessionToken: null,
                RemoteFolderName: "nightly",
                BucketName: "ravendb-backups",
                CustomServerUrl: "https://minio.example.com:9000",
                ForcePathStyle: false,
                DisableChecksumValidation: false,
                StorageClass: "Standard",
            });
        });

        it("trims the Azure, Google Cloud and FTP values but not the FTP password", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: async () => {
                    await wizard.addDestination("Azure", async () => {
                        await wizard.fill("destinations.azure.accountName", " ravendbaccount ");
                        await wizard.fill("destinations.azure.accountKey", " account-key ");
                        await wizard.fill("destinations.azure.storageContainer", " backups ");
                        await wizard.fill("destinations.azure.remoteFolderName", " nightly ");
                    });
                    await wizard.addDestination("Google Cloud", async () => {
                        await wizard.fill("destinations.googleCloud.googleCredentialsJson", googleCredentialsJson);
                        await wizard.fill("destinations.googleCloud.bucketName", " ravendb-backups ");
                        await wizard.fill("destinations.googleCloud.remoteFolderName", " nightly ");
                    });
                    await wizard.addDestination("FTP", async () => {
                        await wizard.fill("destinations.ftp.url", "ftp://ftp.example.com/backups ");
                        await wizard.fill("destinations.ftp.userName", " ftp-user ");
                        await wizard.fill("destinations.ftp.password", " ftp-password ");
                    });
                },
            });

            const dto = await getSavedPeriodicDto();
            expect(dto.AzureSettings).toEqual({
                Disabled: false,
                GetBackupConfigurationScript: undefined,
                AccountName: "ravendbaccount",
                AccountKey: "account-key",
                SasToken: null,
                StorageContainer: "backups",
                RemoteFolderName: "nightly",
            });
            expect(dto.GoogleCloudSettings.BucketName).toBe("ravendb-backups");
            expect(dto.GoogleCloudSettings.RemoteFolderName).toBe("nightly");
            expect(dto.FtpSettings).toEqual({
                Disabled: false,
                GetBackupConfigurationScript: undefined,
                Url: "ftp://ftp.example.com/backups",
                UserName: "ftp-user",
                Password: " ftp-password ",
                CertificateAsBase64: null,
            });
        });

        it("sends the certificate of an FTPS destination with an upper case URL", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("FTP", async () => {
                        await wizard.fill("destinations.ftp.url", "FTPS://ftp.example.com/backups");
                        await wizard.fill("destinations.ftp.userName", "ftp-user");
                        await wizard.fill("destinations.ftp.password", "ftp-password");
                        await wizard.uploadFtpCertificate("certificate");
                    }),
            });

            expect((await getSavedPeriodicDto()).FtpSettings.CertificateAsBase64).toBe("Y2VydGlmaWNhdGU=");
        });

        it("sends an FTP destination with a URL that has no protocol", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("FTP", async () => {
                        await wizard.fill("destinations.ftp.url", "ftp.example.com/backups");
                        await wizard.fill("destinations.ftp.userName", "ftp-user");
                        await wizard.fill("destinations.ftp.password", "ftp-password");
                    }),
            });

            expect((await getSavedPeriodicDto()).FtpSettings).toEqual({
                Disabled: false,
                GetBackupConfigurationScript: undefined,
                Url: "ftp.example.com/backups",
                UserName: "ftp-user",
                Password: "ftp-password",
                CertificateAsBase64: null,
            });
        });

        it("does not add the root path to a local destination that uses an external script", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic", localRootPath: "/srv/backups/" });

            await wizard.completeAllSteps({
                destinations: () => wizard.addDestination("Local", () => wizard.useExternalScript("local")),
            });

            expect((await getSavedPeriodicDto()).LocalSettings).toEqual({
                Disabled: false,
                GetBackupConfigurationScript: backupScript,
                FolderPath: null,
            });
        });

        it("tests the credentials with the same settings that it saves", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });
            const testMock = mockServices.manageServerService.getMock("testPeriodicBackupCredentials");

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("Azure", async () => {
                        await wizard.fill("destinations.azure.accountName", " ravendbaccount ");
                        await wizard.fill("destinations.azure.accountKey", "account-key");
                        await wizard.fireClick(wizard.screen.getByLabelText("SAS token"));
                        await wizard.fill("destinations.azure.sasToken", "sas-token");
                        await wizard.fill("destinations.azure.storageContainer", "backups");
                        await wizard.fireClick(wizard.screen.getByRole("button", { name: "Test credentials" }));
                        expect(
                            await wizard.screen.findByRole("button", { name: "Successfully connected" })
                        ).toBeInTheDocument();
                    }),
            });

            expect(testMock).toHaveBeenCalledTimes(1);
            const [connectionType, testedSettings] = testMock.mock.calls[0];
            expect(connectionType).toBe("Azure");
            expect(toSentJson(testedSettings)).toEqual({
                Disabled: false,
                AccountName: "ravendbaccount",
                AccountKey: null,
                SasToken: "sas-token",
                StorageContainer: "backups",
                RemoteFolderName: null,
            });
            expect(toSentJson(testedSettings)).toEqual((await getSavedPeriodicDto()).AzureSettings);
        });

        it("does not show the session token for a periodic task", async () => {
            const wizard = await renderBackupTask({ mode: "newPeriodic" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("S3 Compatible", async () => {
                        expect(wizard.screen.queryByName("destinations.s3.awsSessionToken")).not.toBeInTheDocument();
                        await wizard.selectByLabel("Region", "Europe (Ireland) - eu-west-1");
                        await wizard.fill("destinations.s3.awsAccessKey", "access-key");
                        await wizard.fill("destinations.s3.awsSecretKey", "secret-key");
                        await wizard.fill("destinations.s3.bucketName", "ravendb-backups");
                    }),
            });

            expect((await getSavedPeriodicDto()).S3Settings.AwsSessionToken).toBeNull();
        });
    });

    describe("manual backup", () => {
        it("sends the default settings", async () => {
            const wizard = await renderBackupTask({ mode: "manual" });

            await wizard.completeAllSteps();

            expect(await getSavedManualDto()).toEqual(manualDto());
        });

        it("sends the snapshot settings, upload mode, read limit and database key", async () => {
            const wizard = await renderBackupTask({ mode: "manual", isDatabaseEncrypted: true });

            await wizard.completeAllSteps({
                backupType: "Snapshot",
                settings: async () => {
                    await wizard.selectByLabel("Backup upload mode", "Direct Upload");
                    await wizard.selectByLabel("Compression level", "No Compression");
                    await wizard.toggle("Exclude indexes data");
                    await wizard.toggle("Set max read operations per second");
                    await wizard.fill("maxReadOpsPerSecond", "100");
                },
            });

            expect(await getSavedManualDto()).toEqual(
                manualDto({
                    BackupType: "Snapshot",
                    BackupUploadMode: "DirectUpload",
                    SnapshotSettings: {
                        CompressionAlgorithm: "Zstd",
                        CompressionLevel: "NoCompression",
                        ExcludeIndexes: true,
                    },
                    BackupEncryptionSettings: { EncryptionMode: "UseDatabaseKey", Key: undefined },
                    MaxReadOpsPerSecond: 100,
                })
            );
        });

        it("sends a relative local path prefixed with the root path set by the administrator", async () => {
            const wizard = await renderBackupTask({ mode: "manual", localRootPath: "/srv/backups/" });

            await wizard.completeAllSteps({
                destinations: () => wizard.addLocalDestination("orders"),
            });

            expect(await getSavedManualDto()).toEqual(
                manualDto({ LocalSettings: localDto({ FolderPath: "/srv/backups/orders" }) })
            );
        });

        it("sends the S3 and Glacier session tokens", async () => {
            const wizard = await renderBackupTask({ mode: "manual" });

            await wizard.completeAllSteps({
                destinations: async () => {
                    await wizard.addDestination("S3 Compatible", async () => {
                        await wizard.selectByLabel("Region", "Europe (Ireland) - eu-west-1");
                        await wizard.fill("destinations.s3.awsAccessKey", "access-key");
                        await wizard.fill("destinations.s3.awsSecretKey", "secret-key");
                        await wizard.fill("destinations.s3.awsSessionToken", " s3-session-token ");
                        await wizard.fill("destinations.s3.bucketName", "ravendb-backups");
                    });
                    await wizard.addDestination("Amazon Glacier", async () => {
                        await wizard.selectByLabel("Region", "Europe (Ireland) - eu-west-1");
                        await wizard.fill("destinations.glacier.awsAccessKey", "access-key");
                        await wizard.fill("destinations.glacier.awsSecretKey", "secret-key");
                        await wizard.fill("destinations.glacier.awsSessionToken", "glacier-session-token");
                        await wizard.fill("destinations.glacier.vaultName", "backups-vault");
                    });
                },
            });

            const dto = await getSavedManualDto();
            expect(dto.S3Settings.AwsSessionToken).toBe("s3-session-token");
            expect(dto.GlacierSettings.AwsSessionToken).toBe("glacier-session-token");
        });

        it("sends null for the session token and the remote folder name after the user clears them", async () => {
            const wizard = await renderBackupTask({ mode: "manual" });

            await wizard.completeAllSteps({
                destinations: () =>
                    wizard.addDestination("Amazon Glacier", async () => {
                        await wizard.selectByLabel("Region", "Europe (Ireland) - eu-west-1");
                        await wizard.fill("destinations.glacier.awsAccessKey", "access-key");
                        await wizard.fill("destinations.glacier.awsSecretKey", "secret-key");
                        await wizard.fill("destinations.glacier.vaultName", "backups-vault");
                        await wizard.fill("destinations.glacier.awsSessionToken", "glacier-session-token");
                        await wizard.fill("destinations.glacier.awsSessionToken", "");
                        await wizard.fill("destinations.glacier.remoteFolderName", "nightly");
                        await wizard.fill("destinations.glacier.remoteFolderName", "  ");
                    }),
            });

            const dto = await getSavedManualDto();
            expect(dto.GlacierSettings.AwsSessionToken).toBeNull();
            expect(dto.GlacierSettings.RemoteFolderName).toBeNull();
        });

        it("does not show the destination state", async () => {
            const wizard = await renderBackupTask({ mode: "manual" });

            await wizard.completeAllSteps({
                destinations: async () => {
                    await wizard.addLocalDestination("/var/backups");
                    expect(wizard.queryDestinationStateButton("Local")).not.toBeInTheDocument();
                },
            });

            expect((await getSavedManualDto()).LocalSettings).toEqual(localDto());
        });
    });

    describe("existing task", () => {
        it("sends back every loaded value and the task id when only the name changes", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({ mode: "editPeriodic", dto });

            await wizard.renameAndSave("Renamed backup");

            expect(await getSavedPeriodicDto()).toEqual({
                ...existingTaskDto(),
                Name: "Renamed backup",
                LocalSettings: { ...dto.LocalSettings, GetBackupConfigurationScript: undefined },
                S3Settings: { ...dto.S3Settings, GetBackupConfigurationScript: undefined },
                GoogleCloudSettings: { ...dto.GoogleCloudSettings, GetBackupConfigurationScript: undefined },
                GlacierSettings: { ...dto.GlacierSettings, GetBackupConfigurationScript: undefined },
                FtpSettings: { ...dto.FtpSettings, GetBackupConfigurationScript: undefined },
            });
        });

        it("keeps the minimum backup age of a disabled retention policy", async () => {
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: existingTaskDto({ RetentionPolicy: { Disabled: true, MinimumBackupAgeToKeep: "2.00:00:00" } }),
            });

            await wizard.renameAndSave("Renamed backup");

            expect((await getSavedPeriodicDto()).RetentionPolicy).toEqual({
                Disabled: true,
                MinimumBackupAgeToKeep: "2.00:00:00",
            });
        });

        it("keeps the local path under the root path set by the administrator", async () => {
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                localRootPath: "/srv/backups/",
                dto: existingTaskDto({ LocalSettings: localDto({ FolderPath: "/srv/backups/orders" }) }),
            });

            await wizard.goToStep("Define destinations");
            await wizard.fireClick(wizard.screen.getByTitle("Edit the Local destination"));
            expect(wizard.screen.getByName("destinations.local.folderPath")).toHaveValue("orders");

            await wizard.renameAndSave("Renamed backup");

            expect((await getSavedPeriodicDto()).LocalSettings).toEqual(
                localDto({ FolderPath: "/srv/backups/orders" })
            );
        });

        it("keeps the certificate of an FTPS destination with an upper case URL", async () => {
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: existingTaskDto({
                    FtpSettings: { ...existingTaskDto().FtpSettings, Url: "FTPS://ftp.example.com/backups" },
                }),
            });

            await wizard.renameAndSave("Renamed backup");

            expect((await getSavedPeriodicDto()).FtpSettings.CertificateAsBase64).toBe("bG9hZGVkLWNlcnRpZmljYXRl");
        });

        it("sends a new encryption key after the user generates one", async () => {
            const wizard = await renderBackupTask({ mode: "editPeriodic", dto: existingTaskDto() });

            await wizard.goToStep("Configure settings");
            await wizard.fireClick(wizard.screen.getByTitle("Generate a new key"));
            await wizard.confirmEncryptionKeySaved();
            await wizard.saveFromCurrentStep();

            expect((await getSavedPeriodicDto()).BackupEncryptionSettings).toEqual({
                EncryptionMode: "UseProvidedKey",
                Key: generatedEncryptionKey,
            });
        });

        it("sends the database key when the task has no encryption settings and the database is encrypted", async () => {
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                isDatabaseEncrypted: true,
                dto: existingTaskDto({ BackupEncryptionSettings: null }),
            });

            await wizard.renameAndSave("Renamed backup");

            expect((await getSavedPeriodicDto()).BackupEncryptionSettings).toEqual({
                EncryptionMode: "UseDatabaseKey",
                Key: undefined,
            });
        });

        it("keeps an unencrypted backup of an encrypted database", async () => {
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                isDatabaseEncrypted: true,
                dto: existingTaskDto({ BackupEncryptionSettings: { EncryptionMode: "None", Key: null } }),
            });

            await wizard.renameAndSave("Renamed backup");

            expect((await getSavedPeriodicDto()).BackupEncryptionSettings).toEqual({
                EncryptionMode: "None",
                Key: undefined,
            });
        });

        it("marks the destination that blocks the next step when its loaded settings are not valid", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: { ...dto, S3Settings: { ...dto.S3Settings, BucketName: "Invalid Bucket" } },
            });

            await wizard.goToStep("Define destinations");
            await wizard.fireClick(wizard.screen.getByRole("button", { name: "Next" }));

            expect(
                await wizard.screen.findByText("This destination has invalid settings. Edit it to fix them.")
            ).toBeInTheDocument();
            expect(wizard.screen.getByRole("heading", { level: 2 })).toHaveTextContent("Define destinations");
        });

        it("drops the custom server settings after the user switches the S3 destination to Amazon AWS", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({ mode: "editPeriodic", dto });

            await wizard.goToStep("Define destinations");
            await wizard.fireClick(wizard.screen.getByTitle("Edit the S3 Compatible destination"));
            await wizard.selectByLabel("Provider", "Amazon AWS");
            await wizard.fireClick(wizard.screen.getByRole("button", { name: "Apply configuration" }));
            await wizard.saveFromCurrentStep();

            expect((await getSavedPeriodicDto()).S3Settings).toEqual({
                ...dto.S3Settings,
                GetBackupConfigurationScript: undefined,
                CustomServerUrl: undefined,
                ForcePathStyle: undefined,
                DisableChecksumValidation: undefined,
            });
        });

        it("drops the external script after the user switches the destination to static credentials", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({ mode: "editPeriodic", dto });

            await wizard.goToStep("Define destinations");
            await wizard.fireClick(wizard.screen.getByTitle("Edit the Azure destination"));
            await wizard.fireClick(wizard.screen.getByRole("radio", { name: "Static credentials" }));
            await wizard.fireClick(wizard.screen.getByRole("button", { name: "Apply configuration" }));
            await wizard.saveFromCurrentStep();

            expect((await getSavedPeriodicDto()).AzureSettings).toEqual({
                ...dto.AzureSettings,
                GetBackupConfigurationScript: undefined,
            });
        });

        it("replaces the destination after the user changes its type in the editor", async () => {
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: existingTaskDto({ AzureSettings: null }),
            });

            await wizard.goToStep("Define destinations");
            await wizard.fireClick(wizard.screen.getByTitle("Edit the FTP destination"));
            await wizard.selectOption(wizard.screen.getByRole("combobox", { name: "Destination" }), "Azure");
            await wizard.useExternalScript("azure");
            await wizard.fireClick(wizard.screen.getByRole("button", { name: "Apply configuration" }));
            expect(await wizard.screen.findByTitle("Remove the Azure destination")).toBeInTheDocument();
            await wizard.saveFromCurrentStep();

            const dto = await getSavedPeriodicDto();
            expect(dto.FtpSettings).toBeUndefined();
            expect(dto.AzureSettings).toEqual({
                Disabled: false,
                GetBackupConfigurationScript: backupScript,
                AccountName: null,
                AccountKey: null,
                SasToken: null,
                StorageContainer: null,
                RemoteFolderName: null,
            });
        });

        it("does not add a storage class to an S3 destination that was saved without one", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: { ...dto, S3Settings: { ...dto.S3Settings, StorageClass: null } },
            });

            await wizard.renameAndSave("Renamed backup");

            expect((await getSavedPeriodicDto()).S3Settings.StorageClass).toBeUndefined();
        });

        it("shows a destination that is disabled in the loaded task and sends it back as disabled", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: { ...dto, S3Settings: { ...dto.S3Settings, Disabled: true } },
            });

            await wizard.goToStep("Define destinations");
            expect(wizard.getDestinationStateButton("S3 Compatible")).toHaveTextContent("Disabled");
            expect(wizard.getDestinationStateButton("Local")).toHaveTextContent("Enabled");
            await wizard.renameAndSave("Renamed backup");

            expect((await getSavedPeriodicDto()).S3Settings).toEqual({
                ...dto.S3Settings,
                Disabled: true,
                GetBackupConfigurationScript: undefined,
            });
        });

        it("sends a destination as disabled with all its settings after the user disables it", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({ mode: "editPeriodic", dto });

            await wizard.goToStep("Define destinations");
            await wizard.setDestinationState("FTP", "Disable");
            await wizard.saveFromCurrentStep();

            expect((await getSavedPeriodicDto()).FtpSettings).toEqual({
                ...dto.FtpSettings,
                Disabled: true,
                GetBackupConfigurationScript: undefined,
            });
        });

        it("sends a destination as enabled after the user enables it", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: { ...dto, GlacierSettings: { ...dto.GlacierSettings, Disabled: true } },
            });

            await wizard.goToStep("Define destinations");
            await wizard.setDestinationState("Amazon Glacier", "Enable");
            await wizard.saveFromCurrentStep();

            expect((await getSavedPeriodicDto()).GlacierSettings).toEqual({
                ...dto.GlacierSettings,
                Disabled: false,
                GetBackupConfigurationScript: undefined,
            });
        });

        it("keeps a destination disabled after the user edits it", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: { ...dto, FtpSettings: { ...dto.FtpSettings, Disabled: true } },
            });

            await wizard.goToStep("Define destinations");
            await wizard.fireClick(wizard.screen.getByTitle("Edit the FTP destination"));
            await wizard.fill("destinations.ftp.userName", "other-user");
            await wizard.fireClick(wizard.screen.getByRole("button", { name: "Apply configuration" }));
            await waitFor(() => expect(wizard.screen.queryByName("destinations.ftp.userName")).not.toBeInTheDocument());
            expect(wizard.getDestinationStateButton("FTP")).toHaveTextContent("Disabled");
            await wizard.saveFromCurrentStep();

            expect((await getSavedPeriodicDto()).FtpSettings).toEqual({
                ...dto.FtpSettings,
                Disabled: true,
                UserName: "other-user",
                GetBackupConfigurationScript: undefined,
            });
        });

        it("does not show or send a disabled destination that has no settings", async () => {
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: existingTaskDto({
                    GlacierSettings: {
                        Disabled: true,
                        GetBackupConfigurationScript: null,
                        AwsRegionName: null,
                        AwsAccessKey: null,
                        AwsSecretKey: null,
                        AwsSessionToken: null,
                        RemoteFolderName: null,
                        VaultName: null,
                    },
                }),
            });

            await wizard.goToStep("Define destinations");
            expect(wizard.screen.queryByLabelText("Amazon Glacier")).not.toBeInTheDocument();
            await wizard.renameAndSave("Renamed backup");

            expect((await getSavedPeriodicDto()).GlacierSettings).toBeUndefined();
        });

        it("does not open the next step when every destination is disabled", async () => {
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: existingTaskDto({
                    S3Settings: null,
                    AzureSettings: null,
                    GoogleCloudSettings: null,
                    GlacierSettings: null,
                    FtpSettings: null,
                }),
            });

            await wizard.goToStep("Define destinations");
            await wizard.setDestinationState("Local", "Disable");
            await wizard.fireClick(wizard.screen.getByRole("button", { name: "Next" }));

            expect(await wizard.screen.findByText("Enable at least one destination to continue")).toBeInTheDocument();
            expect(wizard.screen.getByRole("heading", { level: 2 })).toHaveTextContent("Define destinations");
        });

        it("saves a task with a disabled destination that has invalid settings", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: { ...dto, S3Settings: { ...dto.S3Settings, Disabled: true, BucketName: "Invalid Bucket" } },
            });

            await wizard.renameAndSave("Renamed backup");

            const savedS3Settings = (await getSavedPeriodicDto()).S3Settings;
            expect(savedS3Settings.Disabled).toBe(true);
            expect(savedS3Settings.BucketName).toBe("Invalid Bucket");
        });

        it("marks a destination with invalid settings after the user enables it", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: { ...dto, S3Settings: { ...dto.S3Settings, Disabled: true, BucketName: "Invalid Bucket" } },
            });

            await wizard.goToStep("Define destinations");
            await wizard.setDestinationState("S3 Compatible", "Enable");

            expect(
                await wizard.screen.findByText("This destination has invalid settings. Edit it to fix them.")
            ).toBeInTheDocument();
        });

        it("does not let the user enable a destination that the administrator does not allow", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                allowedDestinations: ["Local", "Azure"],
                dto: {
                    ...dto,
                    S3Settings: { ...dto.S3Settings, Disabled: true },
                    AzureSettings: { ...dto.AzureSettings, Disabled: true },
                },
            });

            await wizard.goToStep("Define destinations");

            expect(wizard.getDestinationStateButton("S3 Compatible")).toBeDisabled();
            expect(wizard.getDestinationStateButton("Azure")).toBeEnabled();
        });

        it("shows the state of each destination in the summary", async () => {
            const dto = existingTaskDto();
            const wizard = await renderBackupTask({
                mode: "editPeriodic",
                dto: { ...dto, S3Settings: { ...dto.S3Settings, Disabled: true } },
            });

            const destinationsSection = within(wizard.screen.getByRole("region", { name: "Destinations" }));

            expect(destinationsSection.getByText("State")).toBeInTheDocument();
            expect(destinationsSection.getAllByText("Disabled")).toHaveLength(1);
            expect(destinationsSection.getAllByText("Enabled")).toHaveLength(5);
        });

        it("does not send a destination after the user removes it", async () => {
            const wizard = await renderBackupTask({ mode: "editPeriodic", dto: existingTaskDto() });

            await wizard.goToStep("Define destinations");
            await wizard.fireClick(wizard.screen.getByTitle("Remove the S3 Compatible destination"));
            await wizard.saveFromCurrentStep();

            const dto = await getSavedPeriodicDto();
            expect(dto.S3Settings).toBeUndefined();
            expect(dto.LocalSettings).toEqual(localDto());
        });
    });
});

interface BackupTaskSetup {
    mode: "newPeriodic" | "editPeriodic" | "manual";
    dto?: PeriodicBackupConfiguration;
    isDatabaseEncrypted?: boolean;
    isSharded?: boolean;
    databaseGroupNodeTags?: string[];
    localRootPath?: string;
    allowedDestinations?: backupOptions[];
    cronServerError?: string;
}

function BackupTaskWithMocks({
    mode,
    dto,
    isDatabaseEncrypted = false,
    isSharded = false,
    databaseGroupNodeTags = null,
    localRootPath = null,
    allowedDestinations = null,
    cronServerError = null,
}: BackupTaskSetup) {
    useState(() => {
        const { databases, license, cluster, accessManager } = mockStore;
        const { tasksService, databasesService, manageServerService } = mockServices;

        accessManager.with_securityClearance("ClusterAdmin");

        const setEncryption = (db: { isEncrypted: boolean }) => {
            db.isEncrypted = isDatabaseEncrypted;
        };
        if (isSharded) {
            databases.withActiveDatabase_Sharded(setEncryption);
        } else {
            databases.withActiveDatabase_NonSharded_Cluster((db) => {
                setEncryption(db);
                if (databaseGroupNodeTags) {
                    db.nodes = db.nodes.filter((node) => databaseGroupNodeTags.includes(node.tag));
                }
            });
        }
        cluster.with_Cluster();
        license.with_License({
            HasPeriodicBackup: true,
            HasSnapshotBackups: true,
            HasCloudBackups: true,
            HasEncryptedBackups: true,
        });

        tasksService.withPeriodicBackupServerLimits((limits) => {
            limits.LocalRootPath = localRootPath;
            limits.AllowedDestinations = allowedDestinations;
        });
        tasksService.withPeriodicBackupConfiguration(dto);
        tasksService.withCronExpressionOccurrence((occurrence) => {
            if (cronServerError) {
                occurrence.IsValid = false;
                occurrence.ErrorMessage = cronServerError;
            }
        });
        tasksService.withBackupLocation();
        tasksService.withLocalFolderPathOptions();
        tasksService.withSavePeriodicBackupConfiguration();
        manageServerService.withTestPeriodicBackupCredentials();
        tasksService.withStartManualBackup();
        databasesService.withGenerateSecret(generatedEncryptionKey);
    });

    return (
        <div id="page-host" className="bs5">
            <EditBackupTask queryParams={getQueryParams(mode)} />
        </div>
    );
}

function getQueryParams(mode: BackupTaskSetup["mode"]) {
    switch (mode) {
        case "newPeriodic":
            return { sourceView: "Backups" as const };
        case "editPeriodic":
            return { sourceView: "Backups" as const, taskId: "7" };
        case "manual":
            return { sourceView: "Backups" as const, manual: "true" };
    }
}

interface WizardSteps {
    backupType?: BackupType | null;
    settings?: () => Promise<void>;
    schedule?: () => Promise<void>;
    destinations?: () => Promise<void>;
}

async function renderBackupTask(setup: BackupTaskSetup) {
    const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<BackupTaskWithMocks {...setup} />);

    const getStepTitle = () => screen.getByRole("heading", { level: 2 }).textContent;

    const next = async () => {
        const stepTitle = getStepTitle();
        await fireClick(screen.getByRole("button", { name: "Next" }));
        await waitFor(() => expect(getStepTitle()).not.toBe(stepTitle));
    };

    const goToStep = (title: string) => fireClick(screen.getByRole("button", { name: title }));

    const toggle = (name: string | RegExp) => fireClick(screen.getByRole("checkbox", { name }));

    const fill = (name: string, value: string) => fillInput(screen.getByName(name), value);

    const selectOption = async (combobox: HTMLElement, optionLabel: string) => {
        fireEvent.keyDown(combobox, { key: "ArrowDown" });
        await fireClick(await screen.findByRole("option", { name: optionLabel }));
    };

    const selectByLabel = (label: string, optionLabel: string) => {
        const field = screen.getByText(label, { selector: "label" }).parentElement;
        return selectOption(within(field).getByRole("combobox"), optionLabel);
    };

    const selectResponsibleNode = async (optionLabel: string) => {
        const toggleSwitch = screen.getByRole("checkbox", { name: "Set Responsible Node" });
        await fireClick(toggleSwitch);
        const settingRow = toggleSwitch.closest<HTMLElement>(".edit-backup-task-settings-row");
        await selectOption(within(settingRow).getByRole("combobox"), optionLabel);
    };

    const confirmEncryptionKeySaved = async () => {
        await waitFor(() => expect(screen.getByLabelText("Encryption key value")).toHaveValue(generatedEncryptionKey));
        await toggle("I have saved the encryption key");
    };

    const useExternalScript = async (destination: string) => {
        await fireClick(screen.getByRole("radio", { name: "External script" }));
        await fill(`destinations.${destination}.config.exec`, backupScript.Exec);
        await fill(`destinations.${destination}.config.arguments`, backupScript.Arguments);
        await fill(`destinations.${destination}.config.timeoutInMs`, String(backupScript.TimeoutInMs));
    };

    const addDestination = async (label: string, configure: () => Promise<void>) => {
        await fireClick(screen.getByRole("button", { name: "Add destination" }));
        await selectOption(screen.getByRole("combobox", { name: "Destination" }), label);
        await configure();
        await fireClick(screen.getByRole("button", { name: "Apply configuration" }));
        expect(await screen.findByTitle(`Remove the ${label} destination`)).toBeInTheDocument();
    };

    const queryDestinationStateButton = (label: string) =>
        within(screen.getByLabelText(label)).queryByTitle(`State of the ${label} destination`);

    const getDestinationStateButton = (label: string) =>
        within(screen.getByLabelText(label)).getByTitle(`State of the ${label} destination`);

    const setDestinationState = async (label: string, action: "Enable" | "Disable") => {
        await fireClick(getDestinationStateButton(label));
        await fireClick(await within(screen.getByLabelText(label)).findByText(action));
    };

    const uploadFtpCertificate = async (content: string) => {
        fireEvent.change(document.getElementById("ftpCertificatePicker"), {
            target: { files: [new File([content], "certificate.pem")] },
        });
        await screen.findByText("<certificate>");
    };

    const addLocalDestination = (folderPath: string) =>
        addDestination("Local", () => fill("destinations.local.folderPath", folderPath));

    const submit = () => fireClick(screen.getByRole("button", { name: /^(Create backup|Save)$/ }));

    const completeAllSteps = async ({ backupType = "Backup", settings, schedule, destinations }: WizardSteps = {}) => {
        if (backupType) {
            await fireClick(screen.getByRole("radio", { name: backupType }));
        }
        await next();

        await settings?.();
        await next();

        if (setup.mode !== "manual") {
            await (schedule ?? (() => toggle("Enable full backup")))();
            await next();
        }

        await (destinations ?? (() => addLocalDestination("/var/backups")))();
        await next();

        await submit();
    };

    const saveFromCurrentStep = async () => {
        while (screen.queryByRole("button", { name: "Next" })) {
            await next();
        }
        await submit();
    };

    const renameAndSave = async (name: string) => {
        await goToStep("Configure settings");
        await fill("name", name);
        await saveFromCurrentStep();
    };

    return {
        screen,
        fireClick,
        fillInput,
        goToStep,
        toggle,
        fill,
        selectOption,
        selectByLabel,
        selectResponsibleNode,
        confirmEncryptionKeySaved,
        useExternalScript,
        queryDestinationStateButton,
        getDestinationStateButton,
        setDestinationState,
        uploadFtpCertificate,
        addDestination,
        addLocalDestination,
        completeAllSteps,
        saveFromCurrentStep,
        renameAndSave,
    };
}

async function getSavedPeriodicDto(): Promise<PeriodicBackupConfiguration> {
    const saveMock = mockServices.tasksService.getMock("savePeriodicBackupConfiguration");
    await waitFor(() => expect(saveMock).toHaveBeenCalledTimes(1));
    return toSentJson(saveMock.mock.calls[0][1]);
}

async function getSavedManualDto(): Promise<BackupConfiguration> {
    const startMock = mockServices.tasksService.getMock("startManualBackup");
    await waitFor(() => expect(startMock).toHaveBeenCalledTimes(1));
    return toSentJson(startMock.mock.calls[0][1]);
}

function toSentJson<T>(dto: T): T {
    return JSON.parse(JSON.stringify(dto));
}

function localDto(
    changes: Partial<Raven.Client.Documents.Operations.Backups.LocalSettings> = {}
): Raven.Client.Documents.Operations.Backups.LocalSettings {
    return {
        Disabled: false,
        GetBackupConfigurationScript: undefined,
        FolderPath: "/var/backups",
        ...changes,
    };
}

function manualDto(changes: Partial<BackupConfiguration> = {}): BackupConfiguration {
    return {
        BackupType: "Backup",
        BackupUploadMode: "Default",
        SnapshotSettings: { CompressionAlgorithm: "Zstd", CompressionLevel: "Fastest", ExcludeIndexes: false },
        BackupEncryptionSettings: { EncryptionMode: "None", Key: undefined },
        MaxReadOpsPerSecond: null,
        LocalSettings: localDto(),
        S3Settings: undefined,
        AzureSettings: undefined,
        GoogleCloudSettings: undefined,
        GlacierSettings: undefined,
        FtpSettings: undefined,
        ...changes,
    };
}

function newPeriodicDto(changes: Partial<PeriodicBackupConfiguration> = {}): PeriodicBackupConfiguration {
    return {
        ...manualDto(),
        TaskId: 0,
        Name: null,
        Disabled: false,
        MentorNode: undefined,
        PinToMentorNode: false,
        FullBackupFrequency: "0 2 * * 0",
        IncrementalBackupFrequency: null,
        RetentionPolicy: { Disabled: true, MinimumBackupAgeToKeep: "1.00:00:00" },
        CreatedAt: null,
        ...changes,
    };
}

function existingTaskDto(changes: Partial<PeriodicBackupConfiguration> = {}): PeriodicBackupConfiguration {
    return {
        TaskId: 7,
        Name: "Nightly backup",
        Disabled: true,
        BackupType: "Backup",
        BackupUploadMode: "DirectUpload",
        FullBackupFrequency: "0 3 * * 1",
        IncrementalBackupFrequency: "*/30 * * * *",
        MentorNode: "B",
        PinToMentorNode: true,
        CreatedAt: "2025-11-14T11:52:05.0963258Z",
        MaxReadOpsPerSecond: 250,
        RetentionPolicy: { Disabled: false, MinimumBackupAgeToKeep: "1.12:00:00" },
        SnapshotSettings: { CompressionAlgorithm: "Deflate", CompressionLevel: "SmallestSize", ExcludeIndexes: true },
        BackupEncryptionSettings: { EncryptionMode: "UseProvidedKey", Key: originalEncryptionKey },
        LocalSettings: { Disabled: false, GetBackupConfigurationScript: null, FolderPath: "/var/backups" },
        S3Settings: {
            Disabled: false,
            GetBackupConfigurationScript: null,
            AwsRegionName: "us-east-1",
            AwsAccessKey: "access-key",
            AwsSecretKey: "secret-key",
            AwsSessionToken: "session-token",
            RemoteFolderName: "nightly",
            BucketName: "ravendb-backups",
            CustomServerUrl: "https://minio.example.com:9000",
            ForcePathStyle: true,
            DisableChecksumValidation: true,
            StorageClass: "Glacier",
        },
        AzureSettings: {
            Disabled: false,
            GetBackupConfigurationScript: backupScript,
            AccountName: "ravendbaccount",
            AccountKey: null,
            SasToken: "sas-token",
            StorageContainer: "backups",
            RemoteFolderName: "nightly",
        },
        GoogleCloudSettings: {
            Disabled: false,
            GetBackupConfigurationScript: null,
            BucketName: "ravendb-backups",
            RemoteFolderName: "nightly",
            GoogleCredentialsJson: googleCredentialsJson,
        },
        GlacierSettings: {
            Disabled: false,
            GetBackupConfigurationScript: null,
            AwsRegionName: "eu-west-1",
            AwsAccessKey: "access-key",
            AwsSecretKey: "secret-key",
            AwsSessionToken: null,
            RemoteFolderName: "nightly",
            VaultName: "backups-vault",
        },
        FtpSettings: {
            Disabled: false,
            GetBackupConfigurationScript: null,
            Url: "ftps://ftp.example.com/backups",
            UserName: "ftp-user",
            Password: "ftp-password",
            CertificateAsBase64: "bG9hZGVkLWNlcnRpZmljYXRl",
        },
        ...changes,
    };
}
