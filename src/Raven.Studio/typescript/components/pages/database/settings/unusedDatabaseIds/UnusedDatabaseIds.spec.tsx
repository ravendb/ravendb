import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { composeStories } from "@storybook/testing-react";
import * as stories from "components/pages/database/settings/unusedDatabaseIds/UnusedDatabaseIds.stories";

const { DefaultUnusedDatabaseIds } = composeStories(stories);

describe("Unused Database IDs", () => {
    it("has full access", async () => {
        const { screen } = rtlRender(<DefaultUnusedDatabaseIds />);

        expect(await screen.findByText("Save")).toBeInTheDocument();
    });
});
