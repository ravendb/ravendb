import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./ServerWideConnectionStrings.stories";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import { mockServices } from "test/mocks/services/MockServices";

const { ServerWideConnectionStringsStory } = composeStories(stories);

describe("ServerWideConnectionStrings", () => {
    beforeEach(() => {
        (mockServices.tasksService.mock.getServerWideConnectionStrings as jest.Mock).mockClear();
    });

    it("fetches connection strings when license has server-wide connection strings", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <ServerWideConnectionStringsStory hasServerWideConnectionStrings />
        );

        expect(mockServices.tasksService.mock.getServerWideConnectionStrings).toHaveBeenCalled();
        expect(screen.queryByTestId("loader")).not.toBeInTheDocument();
    });

    it("does not fetch connection strings and shows empty list without loading placeholder when license has no server-wide connection strings", () => {
        const { screen } = rtlRender(<ServerWideConnectionStringsStory hasServerWideConnectionStrings={false} />);

        expect(mockServices.tasksService.mock.getServerWideConnectionStrings).not.toHaveBeenCalled();
        expect(screen.queryByTestId("loader")).not.toBeInTheDocument();
        expect(screen.getByText(/no server-wide connection strings have been defined/i)).toBeInTheDocument();
    });
});
