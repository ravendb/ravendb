import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentRefresh.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DefaultDocumentRefresh } = composeStories(stories);

describe("DocumentRefresh", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultDocumentRefresh />);

        expect(await screen.findByText("Enable Document Refresh")).toBeInTheDocument();
    });

    it("can disable and set to null refresh frequency after disabling 'Enable Document Refresh'", async () => {
        const { screen, fireClick } = rtlRender(<DefaultDocumentRefresh />);

        const refreshFrequencyBefore = await screen.findByName("refreshFrequency");
        expect(refreshFrequencyBefore).toBeEnabled();
        expect(refreshFrequencyBefore).toHaveValue(DatabasesStubs.refreshConfiguration().RefreshFrequencyInSec);

        await fireClick(screen.getByRole("checkbox", { name: "Enable Document Refresh" }));

        const refreshFrequencyAfter = screen.getByName("refreshFrequency");
        expect(refreshFrequencyAfter).toBeDisabled();
        expect(refreshFrequencyAfter).toHaveValue(null);
    });
});
