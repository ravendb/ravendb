import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/RabbitMqSink.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";

const containerTestId = "sinks";

describe("RabbitMq Sink", function () {
    it("can render enabled", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByText(/RABBITMQ SINK/i)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/)).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        expect(await container.findByText(/Connection String/)).toBeInTheDocument();
        expect(container.getByText("Active")).toBeInTheDocument();
        expect(container.getByText("Source Queues")).toBeInTheDocument();
        expect(container.getByText("orders")).toBeInTheDocument();
    });

    it("can render disabled", async () => {
        const Story = composeStory(stories.Disabled, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        expect(await container.findByText(/Disabled/)).toBeInTheDocument();
        expect(container.queryByText(/Enabled/)).not.toBeInTheDocument();
    });
});
