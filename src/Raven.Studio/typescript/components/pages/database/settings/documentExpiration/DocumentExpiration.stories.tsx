import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentExpiration from "./DocumentExpiration";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Database/Settings/Document Expiration",
    component: DocumentExpiration,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DocumentExpiration>;

function commonInit() {
    const { databasesService } = mockServices;
    databasesService.withExpirationConfiguration();
}

export const DefaultDocumentExpiration: StoryObj<typeof DocumentExpiration> = {
    name: "Document Expiration",
    render: () => {
        commonInit();

        const { license } = mockStore;
        license.with_License();

        return <DocumentExpiration db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};

export const LicenseRestricted: StoryObj<typeof DocumentExpiration> = {
    render: () => {
        commonInit();

        const { license } = mockStore;
        license.with_License({ Type: "Community" });

        return <DocumentExpiration db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
