import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./DocumentSchema.stories";
import { rtlRender } from "test/rtlTestUtils";

const { DefaultDocumentSchema } = composeStories(stories);

describe("DocumentSchema", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultDocumentSchema />);

        const documentSchemaHeading = await screen.findByRole("heading", { name: "Document Schema" });

        expect(documentSchemaHeading).toBeInTheDocument();
    });

    it("shows filter by collection with placeholder", async () => {
        const { screen } = rtlRender(<DefaultDocumentSchema />);

        expect(await screen.findByText("Filter by collection")).toBeInTheDocument();
        expect(screen.getByText("All collections")).toBeInTheDocument();
    });

    it("shows section header and Add new button", async () => {
        const { screen } = rtlRender(<DefaultDocumentSchema />);

        expect(await screen.findByText("Collection specific document schemas")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /Add new/i })).toBeInTheDocument();
    });

    it("shows add new collection section card when user click 'add new' button", async () => {
        const { screen, user } = rtlRender(<DefaultDocumentSchema />);

        expect(screen.getByRole("button", { name: /Add new/i })).toBeInTheDocument();

        await user.click(screen.getByRole("button", { name: /Add new/i }));

        expect(screen.getByText("New Collection Schema")).toBeInTheDocument();
    });
});
