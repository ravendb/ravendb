import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./TombstonesState.stories";
import React from "react";

const { Tombstones } = composeStories(stories);

describe("TombstonesState", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<Tombstones />);

        expect(await screen.findByRole("heading", { name: /Per Collection/ })).toBeInTheDocument();
        expect(await screen.findByRole("heading", { name: /Per Task/ })).toBeInTheDocument();
    });

    it("shows ~ prefix for estimated tombstone counts", async () => {
        const { screen } = rtlRender(<Tombstones />);

        expect(await screen.findByText("~1500")).toBeInTheDocument();
        expect(await screen.findByText("Documents: ~1200, TimeSeries: ~200, Counters: ~100")).toBeInTheDocument();
    });
});
