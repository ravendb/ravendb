import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./CreateSampleData.stories";
import { composeStories } from "@storybook/react";

const { DatabaseWithDocuments, DatabaseWithoutDocuments } = composeStories(stories);

const requiresAnEmptyDatabaseText = "Requires an empty database";
const createSampleDataText = "Create sample data";

describe("CreateSampleData", () => {
    it("can render and submit when documents empty", async () => {
        const { screen, fireClick } = rtlRender(<DatabaseWithoutDocuments />);

        const createButtonBefore = await screen.findByRole("button", { name: createSampleDataText });

        expect(createButtonBefore).toBeEnabled();
        expect(screen.queryByText(requiresAnEmptyDatabaseText)).not.toBeInTheDocument();

        await fireClick(createButtonBefore);

        const createButtonAfter = await screen.findByRole("button", { name: "Sample data created" });

        expect(screen.queryByText(createSampleDataText)).not.toBeInTheDocument();
        expect(createButtonAfter).toBeDisabled();
    });

    it("can render when documents not empty", async () => {
        const { screen } = rtlRender(<DatabaseWithDocuments />);

        const createButton = await screen.findByRole("button", { name: createSampleDataText });

        expect(createButton).toBeDisabled();
        expect(screen.queryByText(requiresAnEmptyDatabaseText)).toBeInTheDocument();
    });
});
