import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentRevisions.stories";
import { documentRevisionsConfigNames } from "./store/documentRevisionsSlice";

const { DefaultDocumentRevisions } = composeStories(stories);

describe("DocumentRevisions", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultDocumentRevisions />);

        expect(await screen.findByText(documentRevisionsConfigNames.defaultDocument)).toBeInTheDocument();
        expect(await screen.findByText(documentRevisionsConfigNames.defaultConflicts)).toBeInTheDocument();
    });
});
