import { composeStory } from "@storybook/react";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/KafkaSink.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";

const containerTestId = "sinks";

describe("Kafka Sink", function () {
    it("can render enabled", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        expect(await container.findByText(/KAFKA SINK/i)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/)).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByText(/Connection String/)).toBeInTheDocument();
    });

    it("can render disabled", async () => {
        const Story = composeStory(stories.Disabled, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        expect(await container.findByText(/Disabled/)).toBeInTheDocument();
        expect(container.queryByText(/Enabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);
        await fireClick(detailsBtn);
    });
});
