import { composeStory } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";
import * as externalReplicationStories from "../stories/ExternalReplication.stories";
import { selectors } from "./selectors";

describe("External Replication", function () {
    it("can render enabled", async () => {
        const Story = composeStory(externalReplicationStories.Default, externalReplicationStories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId("replications"));
        expect(await container.findByRole("heading", { name: "Replication", level: 5 })).toBeInTheDocument();
        expect(await container.findByText(/External Replication/)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/)).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByText(/Connection String/)).toBeInTheDocument();
        expect(await container.findByText(/Destination Database/)).toBeInTheDocument();
        expect(await container.findByText(/Actual Destination URL/)).toBeInTheDocument();
        expect(await container.findByText(/Topology Discovery URLs/)).toBeInTheDocument();

        // edit, delete button should be present for non-server wide
        expect(container.queryByTitle(selectors.deleteTaskTitle)).toBeInTheDocument();
        expect(container.queryByTitle(selectors.editTaskTitle)).toBeInTheDocument();

        expect(await container.findByText(/Last DB Etag/)).toBeInTheDocument();
        expect(await container.findByText(/Last Sent Etag/)).toBeInTheDocument();
    });

    it("can render server wide", async () => {
        const Story = composeStory(externalReplicationStories.ServerWide, externalReplicationStories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId("replications"));
        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        // edit, delete button not present for server wide
        expect(container.queryByTitle(selectors.deleteTaskTitle)).not.toBeInTheDocument();
        expect(container.queryByTitle(selectors.editTaskTitle)).not.toBeInTheDocument();
    });
});
