import { rtlRender } from "test/rtlTestUtils";
import React from "react";

import * as stories from "../stories/AddNewOngoingTask.stories";

import { composeStories } from "@storybook/react-webpack5";
import { fireEvent } from "@testing-library/react";

const { Default } = composeStories(stories);

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
});
