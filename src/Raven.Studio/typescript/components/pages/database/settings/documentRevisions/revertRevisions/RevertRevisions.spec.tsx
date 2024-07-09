import { RtlScreen, rtlRender } from "test/rtlTestUtils";
import * as stories from "./RevertRevisions.stories";
import { composeStories } from "@storybook/react";
import React from "react";

const { DefaultRevertRevisions } = composeStories(stories);

const allCollectionsRadio = "All collections";

describe("RevertRevisions", () => {
    async function waitForLoad(screen: RtlScreen) {
        await screen.findByText("Point in Time");
    }

    it("can render for admin access", async () => {
        const { screen } = rtlRender(<DefaultRevertRevisions databaseAccess="DatabaseAdmin" />);

        await waitForLoad(screen);

        expect(screen.queryByText(allCollectionsRadio)).toBeInTheDocument();
    });

    it("can render below admin access", async () => {
        const { screen } = rtlRender(<DefaultRevertRevisions databaseAccess="DatabaseRead" />);

        await waitForLoad(screen);

        expect(screen.queryByText(allCollectionsRadio)).not.toBeInTheDocument();
    });
});
