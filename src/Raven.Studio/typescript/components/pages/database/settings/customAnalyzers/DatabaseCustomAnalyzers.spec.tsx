import { rtlRender } from "test/rtlTestUtils";
import * as Stories from "./DatabaseCustomAnalyzers.stories";
import { composeStories } from "@storybook/react";
import React from "react";

const { DatabaseCustomAnalyzersStory } = composeStories(Stories);

const selectors = {
    editButtonClass: "icon-edit",
    deleteButtonClass: "icon-trash",
    previewButtonClass: "icon-preview",
};

describe("DatabaseCustomAnalyzers", () => {
    it("can render empty list", async () => {
        const { screen, waitForLoad } = rtlRender(<DatabaseCustomAnalyzersStory isEmpty={true} />);
        await waitForLoad();

        expect(screen.queryByText(/No custom analyzers have been defined/)).toBeInTheDocument();
        expect(screen.queryByText(/No server-wide custom analyzers have been defined/)).toBeInTheDocument();
    });

    it("can render server wide badge when feature is not in license", async () => {
        const { screen, waitForLoad } = rtlRender(
            <DatabaseCustomAnalyzersStory hasServerWideCustomAnalyzers={false} />
        );
        await waitForLoad();

        expect(screen.queryByText(/Professional +/)).toBeInTheDocument();
    });

    it("can render license limit alerts and disable add button", async () => {
        const { screen, waitForLoad } = rtlRender(
            <DatabaseCustomAnalyzersStory
                maxNumberOfCustomAnalyzersPerDatabase={1}
                maxNumberOfCustomAnalyzersPerCluster={4}
            />
        );
        await waitForLoad();

        const [databaseAlert, clusterAlert] = screen.getAllByRole("alert");

        expect(databaseAlert.textContent).toContain(
            "Database has reached the maximum number of Custom Analyzers allowed per database by your license (1/1)"
        );
        expect(clusterAlert.textContent).toContain(
            "Cluster has reached the maximum number of Custom Analyzers allowed per cluster by your license (4/4)"
        );

        expect(screen.getByRole("button", { name: /Add a custom analyzer/ })).toBeDisabled();
    });

    it("can render edit and delete button for access database admin", async () => {
        const { screen, waitForLoad } = rtlRender(<DatabaseCustomAnalyzersStory databaseAccess="DatabaseAdmin" />);
        await waitForLoad();

        expect(screen.queryByClassName(selectors.editButtonClass)).toBeInTheDocument();
        expect(screen.queryByClassName(selectors.deleteButtonClass)).toBeInTheDocument();

        expect(screen.queryByClassName(selectors.previewButtonClass)).not.toBeInTheDocument();
    });

    it("can hide edit and delete buttons for access below database admin", async () => {
        const { screen, waitForLoad } = rtlRender(<DatabaseCustomAnalyzersStory databaseAccess="DatabaseRead" />);
        await waitForLoad();

        expect(screen.queryByClassName(selectors.editButtonClass)).not.toBeInTheDocument();
        expect(screen.queryByClassName(selectors.deleteButtonClass)).not.toBeInTheDocument();

        expect(screen.queryByClassName(selectors.previewButtonClass)).toBeInTheDocument();
    });
});
