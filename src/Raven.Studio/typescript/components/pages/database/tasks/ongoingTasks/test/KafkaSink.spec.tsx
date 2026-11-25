import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/KafkaSink.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";

const containerTestId = "sinks";

describe("Kafka Sink", function () {
    it("can render enabled", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        expect(await container.findByText(/KAFKA SINK/i)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/)).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        expect(await container.findByText(/Connection String/)).toBeInTheDocument();
        expect(container.getByText("Active")).toBeInTheDocument();
        expect(container.getByText("Source Topics")).toBeInTheDocument();
        expect(container.getByText("orders, payments")).toBeInTheDocument();
        expect(container.getByText("1 / 2 enabled")).toBeInTheDocument();
    });

    it("shows when the sink keeps reconnecting", async () => {
        const Story = composeStory(stories.Reconnecting, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        expect(await container.findByText("Reconnect")).toHaveClass("text-warning");
    });

    it("shows when the sink process did not start", async () => {
        const Story = composeStory(stories.NotStarted, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        expect(await container.findByText("None")).toHaveClass("text-warning");
    });

    it("can render disabled", async () => {
        const Story = composeStory(stories.Disabled, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        expect(await container.findByText(/Disabled/)).toBeInTheDocument();
        expect(container.queryByText(/Enabled/)).not.toBeInTheDocument();
    });
});
