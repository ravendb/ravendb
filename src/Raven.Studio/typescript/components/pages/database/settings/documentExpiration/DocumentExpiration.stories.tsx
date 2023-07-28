import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentExpiration from "./DocumentExpiration";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings",
    component: DocumentExpiration,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DocumentExpiration>;

export const DefaultDocumentExpiration: StoryObj<typeof DocumentExpiration> = {
    name: "Document Expiration",
    render: () => {
        const { databasesService } = mockServices;
        databasesService.withExpirationConfiguration();

        return <DocumentExpiration db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
