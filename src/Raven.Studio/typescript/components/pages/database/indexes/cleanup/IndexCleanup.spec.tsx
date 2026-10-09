import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./IndexCleanup.stories";
import moment from "moment";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import { within } from "@testing-library/dom";

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

    it("shows last query and indexing times of merge candidates in local time", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<CleanupSuggestions />);

        const { LastQueryingTime, LastIndexingTime } = IndexesStubs.getSampleStats().find(
            (x) => x.Name === "Product/Search"
        );
        const toLocalDate = (date: string) => `(${moment(date).format("MM/DD/YY, h:mma")})`;

        const [mergeCandidateRow] = screen.getAllByRole("row", { name: /Product\/Search/ });
        const [, lastQueryCell, lastIndexingCell] = within(mergeCandidateRow).getAllByRole("cell");

        expect(lastQueryCell).toHaveTextContent(toLocalDate(LastQueryingTime));
        expect(lastIndexingCell).toHaveTextContent(toLocalDate(LastIndexingTime));
    });

    it("is license restricted", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<LicenseRestricted />);

        const licensingText = await screen.findByText(/Licensing/i);
        expect(licensingText).toBeInTheDocument();
    });

    it("can render nav items", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<CleanupSuggestions />);

        expect(screen.getByRole("heading", { level: 2, name: /Merge indexes/ })).toBeInTheDocument();
        expect(screen.getByRole("heading", { level: 2, name: /Remove sub-indexes/ })).toBeInTheDocument();
        expect(screen.getByRole("heading", { level: 2, name: /Remove unused indexes/ })).toBeInTheDocument();
        expect(screen.getByRole("heading", { level: 2, name: /Unmergeable indexes/ })).toBeInTheDocument();
        expect(screen.getByRole("heading", { level: 2, name: /Merge suggestions errors/ })).toBeInTheDocument();
    });
});
