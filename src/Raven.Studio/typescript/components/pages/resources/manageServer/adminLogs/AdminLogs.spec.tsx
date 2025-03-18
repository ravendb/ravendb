import { composeStories } from "@storybook/react";
import * as Stories from "./AdminLogs.stories";
import { rtlRender } from "test/rtlTestUtils";

const { AdminLogsStory } = composeStories(Stories);

describe("AdminLogs", () => {
    beforeAll(() => {
        Object.defineProperty(HTMLElement.prototype, "scrollWidth", {
            configurable: true,
            value: 500,
        });
        Object.defineProperty(HTMLElement.prototype, "scrollHeight", {
            configurable: true,
            value: 500,
        });
        Object.defineProperty(HTMLElement.prototype, "scrollTo", {
            configurable: true,
            value: () => {},
        });
    });

    it("can render", async () => {
        const { screen } = rtlRender(<AdminLogsStory />);

        expect(await screen.findByText(/Logs on this view/i)).toBeInTheDocument();
    });
});
