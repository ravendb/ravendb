import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import * as Stories from "./GatherDebugInfo.stories";
import { composeStories } from "@storybook/react";

const { DefaultGatherDebugInfo } = composeStories(Stories);

describe("GatherDebugInfo", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultGatherDebugInfo />);
        expect(await screen.findByText("Create package for")).toBeInTheDocument();
    });
});
