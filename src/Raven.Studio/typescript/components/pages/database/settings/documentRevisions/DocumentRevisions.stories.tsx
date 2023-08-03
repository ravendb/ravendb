import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentRevisions from "./DocumentRevisions";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings",
    component: DocumentRevisions,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DocumentRevisions>;

export const DefaultDocumentRevisions: StoryObj<typeof DocumentRevisions> = {
    name: "Document Revisions",
    render: () => {
        return <DocumentRevisions />;
    },
};
