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

function commonInit() {
    const { databasesService } = mockServices;
    databasesService.withRefreshConfiguration();
}

export const DefaultDocumentRefresh: StoryObj<typeof DocumentRefresh> = {
    name: "Document Refresh",
    render: () => {
        commonInit();

        const { license } = mockStore;
        license.with_License();

        return <DocumentRefresh db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};

export const LicenseRestricted: StoryObj<typeof DocumentRefresh> = {
    render: () => {
        commonInit();

        const { license } = mockStore;
        license.with_License({ Type: "Community" });

        return <DocumentRefresh db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
