import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { within } from "@testing-library/dom";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./OverridableField.stories";

const { States, Submitting } = composeStories(stories);

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

    it("enables the input only while overridden", async () => {
        const { screen, user } = rtlRender(<States />);

        const overrideSwitch = await screen.findByRole("checkbox", { name: "Override Override off" });
        expect(screen.getByName("offValue")).toBeDisabled();

        await user.click(overrideSwitch);
        expect(screen.getByName("offValue")).toBeEnabled();

        await user.click(overrideSwitch);
        expect(screen.getByName("offValue")).toBeDisabled();
    });

    it("disables both the switch and the input when disabled", async () => {
        const { screen } = rtlRender(<States />);

        const overrideSwitch = await screen.findByRole("checkbox", { name: "Override Disabled" });
        expect(overrideSwitch).toBeChecked();
        expect(overrideSwitch).toBeDisabled();
        expect(screen.getByName("disabledValue")).toBeDisabled();
    });

    it("resets the value when override is turned off", async () => {
        const { screen, user } = rtlRender(<States />);

        expect(screen.getByName("resetValue")).toHaveValue(45);

        await user.click(await screen.findByRole("checkbox", { name: "Override Reset on disable" }));

        expect(screen.getByName("resetValue")).toHaveValue(null);
    });

    it("names the duration picker after the field", async () => {
        const { screen } = rtlRender(<States />);

        const durationPicker = await screen.findByRole("group", { name: "Duration" });

        expect(within(durationPicker).getByRole("spinbutton", { name: "Hours" })).toHaveValue(1);
        expect(within(durationPicker).getByRole("spinbutton", { name: "Minutes" })).toHaveValue(0);
        expect(within(durationPicker).getByRole("spinbutton", { name: "Seconds" })).toHaveValue(0);
    });

    it("locks the switch while the form is submitting", async () => {
        const { screen, user } = rtlRender(<Submitting />);

        const overrideSwitch = await screen.findByRole("checkbox", { name: "Override Override on" });
        expect(overrideSwitch).toBeEnabled();

        await user.click(screen.getByRole("button", { name: "Save" }));

        expect(overrideSwitch).toBeDisabled();
    });
});
