import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ServerWideCustomAnalyzers from "./ServerWideCustomAnalyzers";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/ManageServer",
    component: ServerWideCustomAnalyzers,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ServerWideCustomAnalyzers>;

export const Default: StoryObj<typeof ServerWideCustomAnalyzers> = {
    name: "Server-Wide Analyzers",
    render: () => {
        const { manageServerService } = mockServices;
        manageServerService.withGetServerWideCustomAnalyzers();

        return <ServerWideCustomAnalyzers />;
    },
};
