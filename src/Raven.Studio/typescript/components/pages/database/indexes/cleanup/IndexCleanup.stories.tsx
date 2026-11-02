import { Meta, StoryFn, StoryObj } from "@storybook/react-webpack5";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { IndexCleanup } from "./IndexCleanup";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Indexes/Index Cleanup",
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/LNQwPn3xNueVK0Wd8U303B/Pages---Index-Cleanup?node-id=0-1&t=6ZsbOaaaRqnEJeXm-1",
        },
    },
} satisfies Meta<typeof IndexCleanup>;

function commonInit() {
    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode();
}

export const EmptyView: StoryFn = () => {
    commonInit();

    const { license } = mockStore;
    license.with_License();

    const { indexesService } = mockServices;

    indexesService.withGetStats([]);
    indexesService.withGetIndexMergeSuggestions({
        Suggestions: [],
        Unmergables: {},
        Errors: [],
    });

    return <IndexCleanup />;
};

export const CleanupSuggestions: StoryFn = () => {
    commonInit();

    const { license } = mockStore;
    license.with_License();

    const { indexesService } = mockServices;

    indexesService.withGetStats();
    indexesService.withGetIndexMergeSuggestions();

    return <IndexCleanup />;
};

export const LicenseRestricted: StoryFn = () => {
    commonInit();

    const { license } = mockStore;
    license.with_LicenseLimited({ HasIndexCleanup: false });

    const { indexesService } = mockServices;

    indexesService.withGetStats();
    indexesService.withGetIndexMergeSuggestions();

    return <IndexCleanup />;
};

interface CleanupSuggestionsInClusterArgs {
    nodeTagWithLatestTimes: string;
    latestTime: string;
}

export const CleanupSuggestionsInCluster: StoryObj<CleanupSuggestionsInClusterArgs> = {
    render: ({ nodeTagWithLatestTimes, latestTime }) => {
        const { databases, license } = mockStore;
        databases.withActiveDatabase_NonSharded_Cluster();
        license.with_License();

        const { indexesService } = mockServices;

        indexesService.withGetStatsPerLocation((stats, location) => {
            if (location.nodeTag !== nodeTagWithLatestTimes) {
                return;
            }

            const productSearch = stats.find((x) => x.Name === "Product/Search");
            productSearch.LastQueryingTime = latestTime;
            productSearch.LastIndexingTime = latestTime;
        });
        indexesService.withGetIndexMergeSuggestions();

        return <IndexCleanup />;
    },
    args: {
        nodeTagWithLatestTimes: "B",
        latestTime: "2025-03-01T08:30:00.0000000Z",
    },
};
