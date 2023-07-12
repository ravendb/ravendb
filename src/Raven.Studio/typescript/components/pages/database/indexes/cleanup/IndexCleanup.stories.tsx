import { Meta, Story } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { IndexCleanup } from "./IndexCleanup";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";

export default {
    title: "Pages/Indexes/Index Cleanup",
    component: IndexCleanup,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof IndexCleanup>;

export const Empty: Story<typeof IndexCleanup> = () => {
    return <IndexCleanup db={DatabasesStubs.nonShardedClusterDatabase()} />;
};

export const CleanupSuggestions: Story<typeof IndexCleanup> = () => {
    const { indexesService } = mockServices;
    indexesService.withGetStats();
    indexesService.withGetIndexMergeSuggestions();

    return <IndexCleanup db={DatabasesStubs.nonShardedClusterDatabase()} />;
};
