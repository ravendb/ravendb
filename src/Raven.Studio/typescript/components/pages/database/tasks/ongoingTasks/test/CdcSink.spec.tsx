import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/CdcSink.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";

const CONTAINER_TEST_ID = "sinks";

describe("CDC Sink", function () {
    it("shows the connection status in the summary row", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(CONTAINER_TEST_ID));

        expect(await container.findByText("Connection Status")).toBeInTheDocument();
        expect(container.getByText("Active")).toBeInTheDocument();
        expect(container.queryByText("Health Issue")).not.toBeInTheDocument();
    });

    it("shows reconnect details under the connection status", async () => {
        const Story = composeStory(stories.Reconnecting, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(CONTAINER_TEST_ID));

        expect(container.queryByText("Health Issue")).not.toBeInTheDocument();

        await fireClick(await container.findByTitle("Show reconnect details"));

        expect(await screen.findByText(/Process is in error recovery/)).toBeInTheDocument();
    });

    it("shows a health issue on its own while the connection is active", async () => {
        const Story = composeStory(stories.HealthIssue, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(CONTAINER_TEST_ID));

        expect(await container.findByText("Health Issue")).toBeInTheDocument();
        expect(container.getByText("Active")).toBeInTheDocument();

        await fireClick(container.getByTitle("Show health issue"));

        expect(await screen.findByRole("tooltip")).toHaveTextContent(/No binlog activity/);
    });

    it("shows the runtime error", async () => {
        const Story = composeStory(stories.RuntimeError, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(CONTAINER_TEST_ID));

        await fireClick(await container.findByRole("button", { name: "Error" }));

        expect(await screen.findByRole("dialog")).toHaveTextContent("CDC Sink process 'CdcSinkTask' was not found.");
    });
});
