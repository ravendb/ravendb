import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentRefresh.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DefaultDocumentRefresh, LicenseRestricted, InitialDocumentRefresh } = composeStories(stories);

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

    it("can set default batch size", async () => {
        const { screen, fireClick } = rtlRender(<InitialDocumentRefresh />);
        const enableButton = await screen.findByRole("checkbox", { name: "Enable Document Refresh" });

        expect(enableButton).not.toBeChecked();

        await fireClick(enableButton);

        const setMaxNumberOfDocumentToProcessCheckbox = await screen.findByLabelText(
            "Set max number of documents to process in a single run"
        );
        expect(setMaxNumberOfDocumentToProcessCheckbox).toBeChecked();
        expect(await screen.findByName("maxItemsToProcess")).toHaveValue(65536);
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });

    it("is limit alert visible", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        const customRefreshFrequency = await screen.findByName("refreshFrequency");
        expect(customRefreshFrequency).toBeEnabled();
        expect(customRefreshFrequency).toHaveValue(DatabasesStubs.refreshConfiguration().RefreshFrequencyInSec);

        const isAlertVisible = screen.getByText(
            "Your current license does not allow a frequency higher than 36 hours (129600 seconds)"
        );
        expect(isAlertVisible).toBeInTheDocument();
    });
});
