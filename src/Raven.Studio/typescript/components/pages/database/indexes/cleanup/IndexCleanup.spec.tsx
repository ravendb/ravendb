import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./IndexCleanup.stories";

const { EmptyView, CleanupSuggestions, LicenseRestricted } = composeStories(stories);

describe("IndexCleanup", function () {
    it("can render empty view", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<EmptyView />);

        expect(screen.getByText("No indexes to merge")).toBeInTheDocument();
    });

    it("can render suggestions", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<CleanupSuggestions />);

        expect(screen.getByText("Review suggested merge")).toBeInTheDocument();
    });
    it("is license restricted", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<LicenseRestricted />);

        fireClick(screen.getByText("Info Hub"));

        const licensingText = await screen.findByText(/Licensing/i);
        expect(licensingText).toBeInTheDocument();
    });

    it("can render nav items", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<CleanupSuggestions />);

        expect(screen.getByRole("heading", { level: 2, name: /Merge indexes/ })).toBeInTheDocument();
        expect(screen.getByRole("heading", { level: 2, name: /Remove sub-indexes/ })).toBeInTheDocument();
        expect(screen.getByRole("heading", { level: 2, name: /Remove unused indexes/ })).toBeInTheDocument();
        expect(screen.getByRole("heading", { level: 2, name: /Unmergable indexes/ })).toBeInTheDocument();
        expect(screen.getByRole("heading", { level: 2, name: /Merge suggestions errors/ })).toBeInTheDocument();
    });
});
