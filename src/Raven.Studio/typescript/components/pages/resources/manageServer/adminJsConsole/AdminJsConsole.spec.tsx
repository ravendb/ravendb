import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/testing-react";
import * as stories from "./AdminJsConsole.stories";

const { JSConsole } = composeStories(stories);

describe("AdminJsConsole", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<JSConsole />);

        expect(await screen.findAllByText("Press Shift+F11 to enter full screen mode")).toHaveLength(2);
    });
});
