import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import * as stories from "./AceEditor.stories";
import { composeStories } from "@storybook/react";

const { JavascriptEditor } = composeStories(stories);

describe("AdminJsConsole", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<JavascriptEditor />);

        expect(await screen.findByText("Press Shift+F11 to enter full screen mode")).toBeInTheDocument;
    });
});
