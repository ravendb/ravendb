import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import { mockServices } from "test/mocks/services/MockServices";
import React from "react";
import StorageReport from "components/pages/resources/manageServer/storageReport/StorageReport";

export default {
    title: "Pages/ManageServer/Storage Report",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

export const StorageReportStory: StoryObj = {
    name: "Storage Report",
    render: () => {
        const { manageServerService } = mockServices;

        manageServerService.withServerWideStorageReport();

        return <StorageReport />;
    },
};
