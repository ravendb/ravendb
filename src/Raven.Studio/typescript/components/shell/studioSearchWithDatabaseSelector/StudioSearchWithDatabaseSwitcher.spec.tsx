import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import * as stories from "./StudioSearchWithDatabaseSwitcher.stories";
import { composeStories } from "@storybook/react";

const { DefaultStory } = composeStories(stories);

describe("StudioSearchWithDatabaseSwitcher", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultStory hasMenuItems={false} isDatabaseSelected={false} />);

        expect(await screen.findByPlaceholderText("Use Ctrl + K to search")).toBeInTheDocument();
        expect(await screen.findByText("No database selected")).toBeInTheDocument();
    });
});
