import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./OverridableField.stories";

const { States } = composeStories(stories);

describe("OverridableField", () => {
    it("names the switch after the field", async () => {
        const { screen } = rtlRender(<States />);

        expect(await screen.findByRole("checkbox", { name: "Override With addon" })).toBeChecked();
    });

    it("focuses the input when its label is clicked", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.click(await screen.findByText("With addon"));

        expect(screen.getByName("addonValue")).toHaveFocus();
    });

    it("focuses the select when its label is clicked", async () => {
        const { screen, user } = rtlRender(<States />);

        await user.click(await screen.findByRole("checkbox", { name: "Override Select" }));
        await user.click(screen.getByText("Select"));

        expect(screen.getByRole("combobox", { name: "Select" })).toHaveFocus();
    });
});
