import { rtlRender, within } from "test/rtlTestUtils";
import React from "react";

import * as stories from "../stories/AddNewOngoingTask.stories";

import { composeStories } from "@storybook/react-webpack5";
import { fireEvent } from "@testing-library/react";

const { Default, CloudProductionFeaturesNotTurnedOn } = composeStories(stories);

describe("AddNewOngoingTask", function () {
    beforeEach(() => {
        localStorage.clear();
    });

    it("can render", async () => {
        const { screen } = rtlRender(<Default />);

        expect(await screen.findByText(/External Replication/)).toBeInTheDocument();
    });

    it("can switch to compact view", async () => {
        const { screen, container } = rtlRender(<Default />);

        await screen.findByText(/External Replication/);

        const taskDescription = /Create a live replica of your database/;
        expect(screen.getByText(taskDescription)).toBeInTheDocument();

        const compactRadio = container.querySelector<HTMLInputElement>("#radio-toggle-right");
        fireEvent.click(compactRadio);

        expect(compactRadio).toBeChecked();
        expect(screen.queryByText(taskDescription)).not.toBeInTheDocument();
    });

    it("names the Cloud feature to turn on only for tasks that have a Cloud switch", async () => {
        const { screen, user } = rtlRender(<CloudProductionFeaturesNotTurnedOn />);

        const getLicenseBadge = (taskTitle: string) =>
            within(screen.getByRole("heading", { name: taskTitle }).closest(".add-task-card")).getByTestId(
                "license-restricted-badge"
            ).parentElement;

        await screen.findByRole("heading", { name: "Kafka ETL" });

        await user.hover(getLicenseBadge("Kafka ETL"));
        expect(await screen.findByText(/^Queue ETL is not turned on for your Cloud instance/)).toBeInTheDocument();

        await user.unhover(getLicenseBadge("Kafka ETL"));
        await user.hover(getLicenseBadge("CDC Sink"));
        expect(await screen.findByText("Not included in your Cloud plan")).toBeInTheDocument();
        expect(screen.queryByText(/CDC Sink is not turned on|Turn it on in Cloud Portal/)).not.toBeInTheDocument();
    });
});
