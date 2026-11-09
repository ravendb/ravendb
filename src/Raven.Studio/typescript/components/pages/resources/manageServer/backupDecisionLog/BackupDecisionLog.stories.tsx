import { Meta, StoryObj } from "@storybook/react-webpack5";
import { userEvent } from "storybook/test";
import BackupDecisionLog from "components/pages/resources/manageServer/backupDecisionLog/BackupDecisionLog";
import { BackupDecisionLogDetails } from "components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogTypes";
import moment from "moment";
import { MockedValue } from "test/mocks/services/AutoMockService";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default {
    title: "Pages/ManageServer/Backup Decision Log",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface BackupDecisionLogStoryArgs {
    isLoadFailing: boolean;
    details: MockedValue<BackupDecisionLogDetails>;
}

export const BackupDecisionLogStory: StoryObj<BackupDecisionLogStoryArgs> = {
    name: "Backup Decision Log",
    render: (args) => {
        const { manageServerService } = mockServices;

        mockStore.cluster.with_Cluster();

        if (args.isLoadFailing) {
            manageServerService.withThrowingGetBackupDecisionLog();
        } else {
            manageServerService.withBackupDecisionLog(args.details);
        }

        return <BackupDecisionLog />;
    },
    args: {
        isLoadFailing: false,
        details: ManageServerStubs.backupDecisionLog(),
    },
};

export const Empty: StoryObj<BackupDecisionLogStoryArgs> = {
    ...BackupDecisionLogStory,
    args: {
        ...BackupDecisionLogStory.args,
        details: { ...ManageServerStubs.backupDecisionLog(), Entries: [], TotalResults: 0 },
    },
};

export const LoadError: StoryObj<BackupDecisionLogStoryArgs> = {
    ...BackupDecisionLogStory,
    args: {
        ...BackupDecisionLogStory.args,
        isLoadFailing: true,
    },
};

export const WithDatabaseFilter: StoryObj<BackupDecisionLogStoryArgs> = {
    ...BackupDecisionLogStory,
    play: async ({ canvas }) => {
        await userEvent.click(await canvas.findByRole("button", { name: /Filter/ }));
        await userEvent.click(await canvas.findByRole("button", { name: "Database" }));
        await userEvent.click(await canvas.findByLabelText("Invoices"));
    },
};

export const ManyCollapsedRepeats: StoryObj<BackupDecisionLogStoryArgs> = {
    ...BackupDecisionLogStory,
    args: {
        ...BackupDecisionLogStory.args,
        details: withEntries((entries) =>
            entries.flatMap((entry, index) =>
                Array.from({ length: 20 * (index + 1) }, (_, repeat) => ({
                    ...entry,
                    Time: moment(entry.Time).add(-repeat, "seconds").toISOString(),
                }))
            )
        ),
    },
};

export const ShardedEntries: StoryObj<BackupDecisionLogStoryArgs> = {
    ...BackupDecisionLogStory,
    args: {
        ...BackupDecisionLogStory.args,
        details: withEntries((entries) =>
            entries.map((entry, index) =>
                entry.Source === "Server" ? entry : { ...entry, Database: `Orders$${index % 3}` }
            )
        ),
    },
};

function withEntries(
    transform: (entries: BackupDecisionLogDetails["Entries"]) => BackupDecisionLogDetails["Entries"]
): BackupDecisionLogDetails {
    const details = ManageServerStubs.backupDecisionLog();
    return { ...details, Entries: transform(details.Entries) };
}
