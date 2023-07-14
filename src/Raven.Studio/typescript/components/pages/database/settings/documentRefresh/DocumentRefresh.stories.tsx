import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentRefresh from "./DocumentRefresh";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings",
    component: DocumentRefresh,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DocumentRefresh>;

export const DefaultDocumentRefresh: StoryObj<typeof DocumentRefresh> = {
    name: "Document Refresh",
    render: () => {
        const { databasesService } = mockServices;
        databasesService.withRefreshConfiguration();

        return <DocumentRefresh db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
