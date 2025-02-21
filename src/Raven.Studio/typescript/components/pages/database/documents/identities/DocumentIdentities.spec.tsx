import { composeStories } from "@storybook/react";
import * as stories from "./DocumentIdentities.stories";
import { rtlRender } from "test/rtlTestUtils";
import React from "react";

const { DocumentIdentitiesStory } = composeStories(stories);

const selectors = {
    newIdentityBtn: "Add new identity",
    documentIdPrefix: "Document ID Prefix",
    tableEditColumn: "Edit",
};

describe("DocumentIdentities", () => {
    it("should be disabled 'Add New Identity' button when database access is read-only", async () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess="DatabaseRead" />);

        const addNewIdentityBtn = await screen.findByRole("button", { name: selectors.newIdentityBtn });
        expect(addNewIdentityBtn).toBeInTheDocument();
        expect(addNewIdentityBtn).toBeDisabled();
    });

    it("should be enabled 'Add New Identity' button when database access is read-write", async () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess="DatabaseReadWrite" />);

        const addNewIdentityBtn = await screen.findByRole("button", { name: selectors.newIdentityBtn });
        expect(addNewIdentityBtn).toBeInTheDocument();
        expect(addNewIdentityBtn).not.toBeDisabled();
    });

    it("should not display the edit column in table when database access is read-only", async () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess="DatabaseRead" />);

        // wait for table load
        expect(await screen.findByText(selectors.documentIdPrefix)).toBeInTheDocument();

        expect(screen.queryByText(selectors.tableEditColumn)).not.toBeInTheDocument();
    });

    it("should display the edit column in table when database access is read-write", async () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess="DatabaseReadWrite" />);

        expect(await screen.findByText(selectors.tableEditColumn)).toBeInTheDocument();
    });
});
