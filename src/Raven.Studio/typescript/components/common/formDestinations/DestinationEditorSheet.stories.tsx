import Button from "react-bootstrap/Button";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { screen, userEvent, within } from "storybook/test";
import {
    securityClearanceArgType,
    withBootstrap5,
    withForceRerender,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { TasksStubs } from "test/stubs/TasksStubs";
import { SharedStubs } from "test/stubs/SharedStubs";
import { useViewSheet, ViewSheet } from "components/common/splitView/ViewSheet";
import DestinationEditorSheet from "./DestinationEditorSheet";
import { DestinationType } from "./utils/formDestinationsTypes";
import { mapDestinationsFromDto } from "./utils/formDestinationsMapsFromDto";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("formDestinations");

const OPEN_EDITOR_LABEL = "Open destination editor";

interface DestinationEditorSheetStoryArgs {
    editedType: DestinationType | "none";
    securityClearance: securityClearance;
    hasCloudBackups: boolean;
    isOnlyLocalAllowed: boolean;
    hasLocalRootPath: boolean;
    isS3StorageClassVisible: boolean;
    isS3CustomProvider: boolean;
    isTestFailing: boolean;
}

type DestinationEditorSheetHostProps = Pick<
    DestinationEditorSheetStoryArgs,
    "editedType" | "isOnlyLocalAllowed" | "hasLocalRootPath" | "isS3StorageClassVisible" | "isS3CustomProvider"
>;

function DestinationEditorSheetHost({
    editedType,
    isOnlyLocalAllowed,
    hasLocalRootPath,
    isS3StorageClassVisible,
    isS3CustomProvider,
}: DestinationEditorSheetHostProps) {
    const { open } = useViewSheet();
    const { destinations } = mapDestinationsFromDto(TasksStubs.periodicBackupConfiguration());
    destinations.s3.isUseCustomHost = isS3CustomProvider;

    const openEditor = () =>
        open({
            initialWidth: 640,
            minWidth: 480,
            component: (
                <ViewSheet className="h-100">
                    <DestinationEditorSheet
                        editedType={editedType === "none" ? null : editedType}
                        destinations={destinations}
                        allowedDestinations={isOnlyLocalAllowed ? ["Local"] : null}
                        localRootPath={hasLocalRootPath ? "/var/lib/ravendb/backups/" : null}
                        isS3StorageClassVisible={isS3StorageClassVisible}
                        onApply={() => null}
                    />
                </ViewSheet>
            ),
        });

    return (
        <div style={{ height: "90vh" }}>
            <Button onClick={openEditor}>{OPEN_EDITOR_LABEL}</Button>
        </div>
    );
}

export default {
    title: "Bits/FormDestinations/DestinationEditorSheet",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        editedType: { control: "radio", options: ["none", "local", "s3", "azure"] },
        securityClearance: securityClearanceArgType,
    },
    args: {
        editedType: "s3",
        securityClearance: "ClusterAdmin",
        hasCloudBackups: true,
        isOnlyLocalAllowed: false,
        hasLocalRootPath: false,
        isS3StorageClassVisible: true,
        isS3CustomProvider: false,
        isTestFailing: false,
    },
    render: ({ securityClearance, hasCloudBackups, isTestFailing, ...hostProps }) => {
        const { accessManager, license, databases } = mockStore;
        const { tasksService, manageServerService } = mockServices;

        databases.withActiveDatabase_NonSharded_SingleNode();
        accessManager.with_securityClearance(securityClearance);
        license.with_License({ HasCloudBackups: hasCloudBackups });
        tasksService.withBackupLocation();
        tasksService.withLocalFolderPathOptions();
        manageServerService.withTestPeriodicBackupCredentials(
            isTestFailing ? SharedStubs.nodeConnectionTestErrorResult() : undefined
        );

        return <DestinationEditorSheetHost {...hostProps} />;
    },
    play: async ({ canvasElement }) => {
        await userEvent.click(await within(canvasElement).findByRole("button", { name: OPEN_EDITOR_LABEL }));
    },
} satisfies Meta<DestinationEditorSheetStoryArgs>;

type Story = StoryObj<DestinationEditorSheetStoryArgs>;

export const Default: Story = {};

export const WithoutS3StorageClass: Story = {
    args: { isS3StorageClassVisible: false },
};

export const S3CustomProvider: Story = {
    args: { isS3CustomProvider: true },
};

export const NewDestination: Story = {
    args: { editedType: "none" },
};

export const LocalWithRootPath: Story = {
    args: { editedType: "local", hasLocalRootPath: true },
};

export const ExternalScript: Story = {
    args: { editedType: "azure" },
};

const openEditorAndTestCredentials: Story["play"] = async ({ canvasElement }) => {
    await userEvent.click(await within(canvasElement).findByRole("button", { name: OPEN_EDITOR_LABEL }));
    await userEvent.click(await screen.findByRole("button", { name: t("testCredentials") }));
};

export const TestSucceeded: Story = {
    play: openEditorAndTestCredentials,
};

export const TestFailed: Story = {
    args: { isTestFailing: true },
    play: openEditorAndTestCredentials,
};

export const LicenseRestricted: Story = {
    args: { editedType: "local", hasCloudBackups: false },
    play: async ({ canvasElement }) => {
        await userEvent.click(await within(canvasElement).findByRole("button", { name: OPEN_EDITOR_LABEL }));
        await userEvent.click(await screen.findByRole("combobox", { name: t("editor.destination") }));
    },
};

export const DisabledByAdministrator: Story = {
    args: { editedType: "local", isOnlyLocalAllowed: true },
};
