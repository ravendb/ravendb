import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./ConnectionStrings.stories";
import { rtlRender_WithWaitForLoad, within } from "test/rtlTestUtils";

const { DefaultConnectionStrings } = composeStories(stories);

const selectors = {
    edit: /edit connection string/i,
    delete: /delete connection string/i,
    addNew: /add new/i,
    emptyList: /no connection strings/i,
    licensing: "Licensing",
};

describe("ConnectionStrings", () => {
    it("can render empty list", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultConnectionStrings isEmpty />);

        expect(screen.getByText(selectors.emptyList)).toBeInTheDocument();
    });

    it("can render connection strings", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultConnectionStrings />);

        expect(screen.queryByText(selectors.emptyList)).not.toBeInTheDocument();
        expect(screen.queryAllByClassName("rich-panel-name")).toHaveLength(12);
    });

    it("can render action buttons when has access database admin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultConnectionStrings databaseAccess="DatabaseAdmin" />);

        // one on the top + one per connection string
        expect(screen.queryAllByRole("button", { name: selectors.addNew })).toHaveLength(12);

        // one per connection string
        expect(screen.queryAllByRole("button", { name: selectors.edit })).toHaveLength(12);
        expect(screen.queryAllByRole("button", { name: selectors.delete })).toHaveLength(12);
    });

    it("can disable action buttons when has access below database admin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultConnectionStrings databaseAccess="DatabaseRead" />);

        const editButtons = screen.queryAllByRole("button", { name: selectors.edit });
        const deleteButtons = screen.queryAllByRole("button", { name: selectors.delete });
        const addNewButtons = screen.queryAllByRole("button", { name: selectors.addNew });

        editButtons.forEach((btn) => expect(btn).toBeDisabled());
        deleteButtons.forEach((btn) => expect(btn).toBeDisabled());
        addNewButtons.forEach((btn) => expect(btn).toBeDisabled());
    });

    it.each([
        { licenseType: "Professional", hasEmbeddingsGeneration: true, availableConnectors: /Anthropic.*OpenAI/ },
        {
            licenseType: "Enterprise",
            hasEmbeddingsGeneration: false,
            availableConnectors: "Available connectors: Embedded (bge-micro-v2)",
        },
    ] satisfies {
        licenseType: Raven.Server.Commercial.LicenseType;
        hasEmbeddingsGeneration: boolean;
        availableConnectors: string | RegExp;
    }[])(
        "can show available AI connectors in license comparison based on embeddings generation for $licenseType license",
        async ({ licenseType, hasEmbeddingsGeneration, availableConnectors }) => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(
                <DefaultConnectionStrings licenseType={licenseType} hasEmbeddingsGeneration={hasEmbeddingsGeneration} />
            );

            await fireClick(screen.getByText(selectors.licensing));

            const currentAiCell = screen.getAllByClassName("current").find((x) => x.tagName === "TD");

            expect(within(currentAiCell).getByText(/\d/, { selector: "sup" })).toHaveAccessibleDescription(
                availableConnectors
            );
        }
    );
});
