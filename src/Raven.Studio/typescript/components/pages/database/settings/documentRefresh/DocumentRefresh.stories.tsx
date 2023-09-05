import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentRefresh from "./DocumentRefresh";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/Document Refresh",
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

export const LicenseRestricted: StoryObj<typeof DocumentRefresh> = {
    render: () => {
        const { databasesService } = mockServices;
        const { license } = mockStore;
        databasesService.withRefreshConfiguration();
        license.with_Community();

        return <DocumentRefresh db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
