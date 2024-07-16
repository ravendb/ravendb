import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/testing-react";
import * as stories from "./StudioSearchWithDatabaseSwitcher.stories";

const { DefaultStory } = composeStories(stories);

describe("StudioSearchWithDatabaseSwitcher", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultStory hasMenuItems={false} isDatabaseSelected={false} />);

        expect(await screen.findByPlaceholderText("Use Ctrl + K to search")).toBeInTheDocument();
        expect(await screen.findByText("No database selected")).toBeInTheDocument();
    });
});
