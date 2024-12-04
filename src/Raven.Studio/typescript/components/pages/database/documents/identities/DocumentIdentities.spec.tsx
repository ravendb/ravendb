import { composeStories } from "@storybook/react";
import * as stories from "./DocumentIdentities.stories";
import { rtlRender } from "test/rtlTestUtils";
import React from "react";

const { DocumentIdentitiesStory } = composeStories(stories);

const selectors = {
    newIdentityBtn: "Add new identity",
    tableEditColumn: "Edit",
};

describe("DocumentIdentities", () => {
    beforeAll(() => {
        Object.defineProperty(HTMLElement.prototype, "scrollWidth", {
            configurable: true,
            value: 500,
        });
    });

    it("should render component with sample data and not be able to click btn", async () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess={"DatabaseRead"} />);

        const addNewIdentityBtn = screen.getByRole("button", { name: selectors.newIdentityBtn });
        expect(addNewIdentityBtn).toBeInTheDocument();
        expect(addNewIdentityBtn).toBeDisabled();
    });

    it("should render component with sample data and not be able to click btn", async () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess={"DatabaseReadWrite"} />);

        const addNewIdentityBtn = screen.getByRole("button", { name: selectors.newIdentityBtn });
        expect(addNewIdentityBtn).toBeInTheDocument();
        expect(addNewIdentityBtn).not.toBeDisabled();
    });

    it("should not see edit column", async () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess={"DatabaseRead"} />);

        expect(screen.queryByText(selectors.tableEditColumn)).not.toBeInTheDocument();
    });

    it("should see edit column", async () => {
        const { screen } = rtlRender(<DocumentIdentitiesStory databaseAccess={"DatabaseReadWrite"} />);

        expect(screen.queryByText(selectors.tableEditColumn)).toBeInTheDocument();
    });
});
