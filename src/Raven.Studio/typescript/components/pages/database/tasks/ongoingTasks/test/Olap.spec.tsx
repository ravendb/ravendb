import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/Olap.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";
import React from "react";

import { selectors } from "./selectors";

const containerTestId = "etls";

describe("OLAP", function () {
    it("can render disabled and not completed", async () => {
        const Story = composeStory(stories.Disabled, stories.default);

        const { screen, fireClick } = rtlRender(<Story disabled completed={false} />);
        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByText(/OLAP ETL/)).toBeInTheDocument();
        expect(await container.findByText(/Disabled/, { selector: "button" })).toBeInTheDocument();
        expect(container.queryByText(/Enabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByTestId("destination")).toBeInTheDocument();

        expect(await container.findByText("Destination Description")).toBeInTheDocument();

        expect(await container.findByText(/Connection String/)).toBeInTheDocument();

        //wait for progress
        await container.findAllByText(/Disabled/i);
    });

    it("can render completed", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story completed />);
        const container = within(await screen.findByTestId(containerTestId));
        const detailsBtn = await container.findByTitle(/Click for details/);
        await fireClick(detailsBtn);

        //wait for progress
        await container.findAllByText(/Up to date/i);
    });

    it("can render enabled and not completed", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story completed={false} disabled={false} />);
        const container = within(await screen.findByTestId(containerTestId));
        const detailsBtn = await container.findByTitle(/Click for details/);
        await fireClick(detailsBtn);

        //wait for progress
        await container.findAllByText("Running");
    });

    it("can notify about empty script", async () => {
        const Story = composeStory(stories.EmptyScript, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        const detailsBtn = await container.findByTitle(/Click for details/);
        await fireClick(detailsBtn);

        //wait for progress
        await container.findAllByText(/Up to date/i);

        expect(await container.findByText(selectors.emptyScriptText)).toBeInTheDocument();
    });
});
