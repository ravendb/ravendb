import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/Subscription.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";

const containerTestId = "subscriptions";

describe("Subscription", function () {
    it("can render enabled", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);

        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByRole("heading", { name: /Subscription/ })).toBeInTheDocument();
        expect(await container.findByText(/Enabled/)).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByText(/Last Batch Ack Time/)).toBeInTheDocument();
        expect(await container.findByText(/Last Client Connection Time/)).toBeInTheDocument();
        expect(await container.findByText(/Change vector for next batch/)).toBeInTheDocument();
    });
});
