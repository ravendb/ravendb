import React from "react";
import { ComponentMeta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DocumentCompression from "./DocumentCompression";

export default {
    title: "Pages/Database/Settings",
    component: DocumentCompression,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof DocumentCompression>;

export const DefaultDocumentCompression: StoryObj<typeof DocumentCompression> = {
    name: "Document Compression",
    render: () => {
        return <DocumentCompression />;
    },
};
