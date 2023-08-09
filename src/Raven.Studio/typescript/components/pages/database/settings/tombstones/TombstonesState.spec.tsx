import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./TombstonesState.stories";

const { Tombstones } = composeStories(stories);

describe("TombstonesState", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<Tombstones />);

        expect(await screen.findByRole("heading", { name: /Per Collection/ })).toBeInTheDocument();
        expect(await screen.findByRole("heading", { name: /Per Task/ })).toBeInTheDocument();
    });
});
