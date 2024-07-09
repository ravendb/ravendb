import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./AdminJsConsole.stories";

const { DefaultAdminJSConsole } = composeStories(stories);

describe("AdminJsConsole", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultAdminJSConsole />);

        expect((await screen.findAllByText("Admin JS Console")).length).toBeGreaterThan(1);
        expect(await screen.findAllByText("Press Shift+F11 to enter full screen mode")).toHaveLength(2);
    });
});
