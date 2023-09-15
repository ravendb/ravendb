import { Meta, Story } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { IndexCleanup } from "./IndexCleanup";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Indexes/Index Cleanup",
    component: IndexCleanup,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof IndexCleanup>;

export const EmptyView: Story<typeof IndexCleanup> = () => {
    const { license } = mockStore;
    license.with_License();

    const { indexesService } = mockServices;

    indexesService.withGetStats([]);
    indexesService.withGetIndexMergeSuggestions({
        Suggestions: [],
        Unmergables: {},
    });

    return <IndexCleanup db={DatabasesStubs.nonShardedClusterDatabase()} />;
};

export const CleanupSuggestions: Story<typeof IndexCleanup> = () => {
    const { license } = mockStore;
    license.with_License();

    const { indexesService } = mockServices;

    indexesService.withGetStats();
    indexesService.withGetIndexMergeSuggestions();

    return <IndexCleanup db={DatabasesStubs.nonShardedClusterDatabase()} />;
};

export const LicenseRestricted: Story<typeof IndexCleanup> = () => {
    const { license } = mockStore;
    license.with_License({ Type: "Community" });

    const { indexesService } = mockServices;

    indexesService.withGetStats();
    indexesService.withGetIndexMergeSuggestions();

    return <IndexCleanup db={DatabasesStubs.nonShardedClusterDatabase()} />;
};
