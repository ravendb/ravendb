import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./ClientGlobalConfiguration.stories";
import { composeStories } from "@storybook/react-webpack5";

const { ClientConfiguration, LicenseRestricted } = composeStories(stories);

describe("ClientGlobalConfiguration", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<ClientConfiguration />);

        expect(await screen.findByText(/Save/)).toBeInTheDocument();
    });

    it("can validate identity parts separator", async () => {
        const { screen, fillInput, fireClick } = rtlRender(<ClientConfiguration />);

        const inputElement = await screen.findByName("identityPartsSeparatorValue");
        const saveButton = screen.getByRole("button", { name: "Save" });

        await fillInput(inputElement, "|");
        await fireClick(saveButton);
        expect(screen.getByText("Identity parts separator cannot be set to '|'")).toBeInTheDocument();

        await fillInput(inputElement, "ab");
        await fireClick(saveButton);
        expect(screen.getByText("Please enter exactly 1 character")).toBeInTheDocument();
    });

    it("links balance behavior labels to their selects", async () => {
        const { screen } = rtlRender(<ClientConfiguration />);

        await screen.findByText(/Save/);

        for (const label of ["Load Balance Behavior", "Read Balance Behavior"]) {
            const select = screen.getByRole("combobox", { name: label });
            expect(screen.getByText(label).closest("label")).toHaveAttribute("for", select.id);
        }
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });
});
