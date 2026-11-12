import { Meta, StoryObj } from "@storybook/react-webpack5";
import { userEvent } from "storybook/test";
import BackupDecisionLog from "components/pages/resources/manageServer/backupDecisionLog/BackupDecisionLog";
import { BackupDecisionLogDetails } from "components/pages/resources/manageServer/backupDecisionLog/backupDecisionLogTypes";
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
            manageServerService.withFailingBackupDecisionLog();
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
        details: withEntries((entries) => entries.map((entry, index) => ({ ...entry, Count: 20 * (index + 1) }))),
    },
};

export const ShardedEntries: StoryObj<BackupDecisionLogStoryArgs> = {
    ...BackupDecisionLogStory,
    args: {
        ...BackupDecisionLogStory.args,
        details: withEntries((entries) =>
            entries.map((entry, index) =>
                entry.Source === "Server" ? entry : { ...entry, Database: "Orders", Shard: index % 3 }
            )
        ),
    },
};

export const ServerBlocked: StoryObj<BackupDecisionLogStoryArgs> = {
    ...BackupDecisionLogStory,
    args: {
        ...BackupDecisionLogStory.args,
        details: (x) => {
            x.Summary.ServerBlock = {
                Detail: "LowMemory",
                Reason: "The server is low on memory, scheduled backups are held back",
                Since: x.RunnerStartTime,
            };
        },
    },
};

function withEntries(
    transform: (entries: BackupDecisionLogDetails["Entries"]) => BackupDecisionLogDetails["Entries"]
): BackupDecisionLogDetails {
    const details = ManageServerStubs.backupDecisionLog();
    return { ...details, Entries: transform(details.Entries) };
}
