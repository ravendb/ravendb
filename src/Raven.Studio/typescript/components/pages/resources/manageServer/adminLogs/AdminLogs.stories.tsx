import { Meta, StoryObj } from "@storybook/react";
import AdminLogs from "components/pages/resources/manageServer/adminLogs/AdminLogs";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";

export default {
    title: "Pages/ManageServer/Admin Logs",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const AdminLogsStory: StoryObj = {
    name: "Admin Logs",
    render: () => {
        const { manageServerService } = mockServices;
        const { databases, adminLogs } = mockStore;

        databases.withActiveDatabase();
        adminLogs.with_logs();

        manageServerService.withAdminLogsConfiguration();
        manageServerService.withEventListenerConfiguration();
        manageServerService.withTrafficWatchConfiguration();

        return (
            <div style={{ height: "90vh" }}>
                <AdminLogs />
            </div>
        );
    },
};
