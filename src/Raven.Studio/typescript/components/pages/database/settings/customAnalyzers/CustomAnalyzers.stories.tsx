import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import CustomAnalyzers from "./CustomAnalyzers";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Database/Settings",
    component: CustomAnalyzers,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof CustomAnalyzers>;

export const DefaultCustomAnalyzers: StoryObj<typeof CustomAnalyzers> = {
    name: "Custom Analyzers",
    render: () => {
        return <CustomAnalyzers db={DatabasesStubs.nonShardedClusterDatabase()} />;
    },
};
