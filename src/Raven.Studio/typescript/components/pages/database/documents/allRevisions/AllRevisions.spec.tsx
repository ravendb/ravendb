import { rtlRender } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react";
import * as Stories from "./AllRevisions.stories";

const { AllRevisionsStory } = composeStories(Stories);

describe("AllRevisions", () => {
    beforeAll(() => {
        Object.defineProperty(HTMLElement.prototype, "scrollWidth", {
            configurable: true,
            value: 500,
        });
        Object.defineProperty(HTMLElement.prototype, "scrollHeight", {
            configurable: true,
            value: 500,
        });
    });

    it("can render", async () => {
        const { screen } = rtlRender(<AllRevisionsStory />);

        expect(await screen.findByText(/Id/i)).toBeInTheDocument();
    });
});
