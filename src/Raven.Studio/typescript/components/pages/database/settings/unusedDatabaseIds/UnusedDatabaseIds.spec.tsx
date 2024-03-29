import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { composeStories } from "@storybook/testing-react";
import * as stories from "components/pages/database/settings/unusedDatabaseIds/UnusedDatabaseIds.stories";

const { UnusedDatabaseIdsStory } = composeStories(stories);

describe("Unused Database IDs", () => {
    // TODO
});
