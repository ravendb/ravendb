import { composeStory } from "@storybook/react";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/ReplicationSink.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";
import { selectors } from "components/pages/database/tasks/ongoingTasks/test/selectors";

const containerTestId = "replications";

describe("Replication Sink", function () {
    it("can render enabled", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByRole("heading", { name: "Replication", level: 5 })).toBeInTheDocument();
        expect(await container.findByText(/Replication Sink/)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/)).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByText(/Hub Database/)).toBeInTheDocument();
        expect(await container.findByText(/Connection String/)).toBeInTheDocument();
        expect(await container.findByText(/Actual Hub URL/)).toBeInTheDocument();
        expect(await container.findByText(/Hub Name/)).toBeInTheDocument();

        // edit, delete button should be present for non-server wide
        expect(container.queryByTitle(selectors.deleteTaskTitle)).toBeInTheDocument();
        expect(container.queryByTitle(selectors.editTaskTitle)).toBeInTheDocument();

        expect(await container.findByText(/Last DB Etag/)).toBeInTheDocument();
        expect(await container.findByText(/Last Sent Etag/)).toBeInTheDocument();
    });
});
