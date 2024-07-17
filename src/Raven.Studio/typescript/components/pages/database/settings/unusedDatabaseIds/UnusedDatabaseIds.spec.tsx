import React from "react";
import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react";
import * as stories from "components/pages/database/settings/unusedDatabaseIds/UnusedDatabaseIds.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { UnusedDatabaseIdsStory } = composeStories(stories);

const selectors = {
    emptyList: /No Unused IDs have been added/,
    databaseId: DatabasesStubs.detailedStats().DatabaseId,
};

describe("Unused Database IDs", () => {
    it("can render empty list", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<UnusedDatabaseIdsStory isEmpty />);

        expect(screen.queryByText(selectors.emptyList)).toBeInTheDocument();
        expect(screen.queryByText(selectors.databaseId)).not.toBeInTheDocument();
    });

    it("can render list with items", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<UnusedDatabaseIdsStory />);

        expect(screen.queryByText(selectors.databaseId)).toBeInTheDocument();
        expect(screen.queryByText(selectors.emptyList)).not.toBeInTheDocument();
    });
});
