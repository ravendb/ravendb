import { rtlRender } from "test/rtlTestUtils";
import React from "react";

import * as stories from "../stories/AddNewOngoingTask.stories";

import { composeStories } from "@storybook/react-webpack5";

const { Default } = composeStories(stories);

describe("AddNewOngoingTask", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<Default />);

        expect(await screen.findByText(/External Replication/)).toBeInTheDocument();
    });
});
