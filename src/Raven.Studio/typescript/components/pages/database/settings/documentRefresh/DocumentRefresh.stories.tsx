import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentRefresh from "./DocumentRefresh";

export default {
    title: "Pages/Database/Settings",
    component: DocumentRefresh,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DocumentRefresh>;

export const DefaultDocumentRefresh: StoryObj<typeof DocumentRefresh> = {
    name: "Document Refresh",
    render: () => {
        return <DocumentRefresh />;
    },
};
