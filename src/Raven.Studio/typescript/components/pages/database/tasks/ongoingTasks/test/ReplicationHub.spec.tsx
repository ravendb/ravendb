import { composeStory } from "@storybook/react";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/ReplicationHub.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";

import React from "react";

const containerTestId = "replications";

describe("Replication Hub", function () {
    it("can render hub w/o connections", async () => {
        const Story = composeStory(stories.NoConnections, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByRole("heading", { name: "Replication", level: 5 })).toBeInTheDocument();
        expect(await container.findByText(/Replication Hub/)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/, { selector: "button" })).toBeInTheDocument();
        expect(container.queryByText(/Disabled/, { selector: "button" })).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByText(/No sinks connected/)).toBeInTheDocument();
    });

    it("can render hub w/ connections", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByRole("heading", { name: "Replication", level: 5 })).toBeInTheDocument();
        expect(await container.findByText(/Replication Hub/)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/, { selector: "button" })).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByText(/Task Name/)).toBeInTheDocument();
        expect(await container.findByText(/Sink Database/)).toBeInTheDocument();
        expect(await container.findByText(/target-hub-db/)).toBeInTheDocument();
        expect(await container.findByText(/Actual Sink URL/)).toBeInTheDocument();

        expect(await container.findByText(/Last DB Etag/)).toBeInTheDocument();
        expect(await container.findByText(/Last Sent Etag/)).toBeInTheDocument();
    });
});
