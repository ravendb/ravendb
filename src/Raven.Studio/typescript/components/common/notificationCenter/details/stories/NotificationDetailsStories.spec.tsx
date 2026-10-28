import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import { mockTableWidth } from "../notificationDetailsTestUtils";
import * as alertStories from "./AlertDetails.stories";
import * as otherStories from "./OtherDetails.stories";
import * as performanceHintStories from "./PerformanceHintDetails.stories";
import * as virtualOperationStories from "./VirtualOperationDetails.stories";

const allStories = {
    ...composeStories(alertStories),
    ...composeStories(otherStories),
    ...composeStories(performanceHintStories),
    ...composeStories(virtualOperationStories),
};

describe("Notification details stories", () => {
    mockTableWidth();

    it.each(Object.entries(allStories))("renders %s", async (_, Story) => {
        const { screen } = rtlRender(<Story />);

        expect(await screen.findByRole("dialog")).toBeInTheDocument();
        expect(screen.getAllByRole("button", { name: "Close" })).toHaveLength(2);
    });
});
