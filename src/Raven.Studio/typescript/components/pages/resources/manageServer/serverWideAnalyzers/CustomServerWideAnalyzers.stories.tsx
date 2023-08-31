import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import ServerWideAnalyzers from "./CustomServerWideAnalyzers";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/ManageServer",
    component: ServerWideAnalyzers,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof ServerWideAnalyzers>;

export const DefaultServerWideAnalyzers: StoryObj<typeof ServerWideAnalyzers> = {
    name: "Server-Wide Analyzers",
    render: () => {
        return <ServerWideAnalyzers db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
