import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentRefresh from "./DocumentRefresh";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/Document Refresh",
    component: DocumentRefresh,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DocumentRefresh>;

function commonInit() {
    const { databasesService } = mockServices;
    const { databases } = mockStore;
    databasesService.withRefreshConfiguration();
    databases.withActiveDatabase_NonSharded_SingleNode();
}

export const DefaultDocumentRefresh: StoryObj<typeof DocumentRefresh> = {
    name: "Document Refresh",
    render: () => {
        commonInit();

        const { license } = mockStore;
        license.with_License();

        return <DocumentRefresh />;
    },
};

export const LicenseRestricted: StoryObj<typeof DocumentRefresh> = {
    render: () => {
        commonInit();

        const { license } = mockStore;
        license.with_LicenseLimited();

        return <DocumentRefresh />;
    },
};
