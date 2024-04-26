import { Meta, StoryFn } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { IndexErrors } from "./IndexErrors";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Indexes/Index Errors",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof IndexErrors>;

function commonInit() {
    const { databases } = mockStore;
    databases.withActiveDatabase_NonSharded_SingleNode();
}

export const ErroredView: StoryFn = () => {
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

    return <IndexErrors />;
};
