import { rtlRender } from "test/rtlTestUtils";
import React from "react";

import * as stories from "../stories/OngoingTasksPage.stories";

import { composeStories } from "@storybook/react-webpack5";

const { FullView } = composeStories(stories);

describe("OngoingTasksPage", function () {
    it("can render full view", async () => {
        const { screen } = rtlRender(<FullView />);

        expect(await screen.findByText(/RavenDB ETL/)).toBeInTheDocument();
    });
});
