import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { NotificationStubs } from "test/stubs/NotificationStubs";
import RecentErrorDetails from "../RecentErrorDetails";
import DumpRawIndexDataDetails from "../operations/DumpRawIndexDataDetails";
import { storyActions } from "./notificationDetailsStoryUtils";

export default {
    title: "Shell/Notification Details/Other",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const RecentError: StoryObj = {
    render: () => {
        const error = NotificationStubs.recentError({
            Title: "Failed to load the index definition",
            Message: "Could not load the index 'Orders/ByCompany'. The index does not exist.",
            Details: JSON.stringify(
                {
                    Url: "/databases/db1/indexes?name=Orders%2FByCompany",
                    Type: "Raven.Client.Exceptions.Documents.Indexes.IndexDoesNotExistException",
                    Message: "Index 'Orders/ByCompany' does not exist.",
                    Error: "Raven.Client.Exceptions.Documents.Indexes.IndexDoesNotExistException: Index 'Orders/ByCompany' does not exist.\n   at Raven.Server.Documents.Handlers.IndexHandler.GetAll()",
                },
                null,
                4
            ),
        });

        return <RecentErrorDetails notification={error} actions={storyActions} />;
    },
};

export const DumpRawIndexDataCompleted: StoryObj = {
    render: () => {
        const operation = NotificationStubs.operation(
            "DumpRawIndexData",
            { Status: "Completed", Progress: null, Result: { Message: "Dumped 12 files", ShouldPersist: false } },
            {
                Title: "Dump raw index data",
                Message: "Raw data of the index 'Orders/ByCompany' was saved to 'C:\\RavenDB\\Dumps'.",
            }
        );

        return <DumpRawIndexDataDetails notification={operation} actions={storyActions} />;
    },
};

export const DumpRawIndexDataFailed: StoryObj = {
    render: () => {
        const operation = NotificationStubs.operation(
            "DumpRawIndexData",
            {
                Status: "Faulted",
                Progress: null,
                Result: {
                    Message: "Could not write to 'C:\\RavenDB\\Dumps'.",
                    Error: "System.UnauthorizedAccessException: Access to the path 'C:\\RavenDB\\Dumps' is denied.",
                },
            },
            {
                Title: "Dump raw index data",
                Message: "Dumping raw data of the index 'Orders/ByCompany' failed.",
            }
        );

        return <DumpRawIndexDataDetails notification={operation} actions={storyActions} />;
    },
};
