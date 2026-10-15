import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentRefresh.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DefaultDocumentRefresh, LicenseRestricted, InitialDocumentRefresh } = composeStories(stories);

const licenseLimitWarning = "Your current license does not allow a refresh frequency below 36 hours (129600 seconds).";
// LicenseStubs limits the minimum period to 36 hours
const licenseMinimumRefreshFrequency = 129600;

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
        expect(screen.getByName("maxItemsToProcess")).toHaveAttribute("placeholder", "Default (unlimited)");

        await fireClick(enableButton);

        const setMaxNumberOfDocumentToProcessCheckbox = await screen.findByRole("checkbox", {
            name: "Override Max number of documents to process in a single run",
        });
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

        expect(screen.getByText(licenseLimitWarning)).toBeInTheDocument();
    });

    it("shows the server default refresh frequency", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        await screen.findByName("refreshFrequency");

        expect(screen.getByName("refreshFrequency")).toHaveAttribute("placeholder", "Default (60)");
    });

    it("turns refresh frequency on when the license enforces a minimum period", async () => {
        const { screen, fireClick } = rtlRender(<LicenseRestricted />);

        const documentRefreshSwitch = await screen.findByRole("checkbox", { name: "Enable Document Refresh" });

        await fireClick(documentRefreshSwitch);
        expect(screen.getByRole("checkbox", { name: "Override Refresh frequency" })).not.toBeChecked();

        await fireClick(documentRefreshSwitch);
        expect(screen.getByRole("checkbox", { name: "Override Refresh frequency" })).toBeChecked();
        expect(screen.getByName("refreshFrequency")).toHaveValue(licenseMinimumRefreshFrequency);
        expect(screen.queryByText(licenseLimitWarning)).not.toBeInTheDocument();
    });

    it("blocks saving when the frequency falls back to the server default on a limited license", async () => {
        const { screen, fireClick, user } = rtlRender(<LicenseRestricted />);

        await fireClick(await screen.findByRole("checkbox", { name: "Override Refresh frequency" }));

        expect(screen.getByRole("checkbox", { name: "Override Refresh frequency" })).not.toBeChecked();
        expect(screen.getByText(licenseLimitWarning)).toBeInTheDocument();

        const saveButton = screen.getByRole("button", { name: "Save" });
        expect(saveButton).toBeDisabled();

        await user.hover(saveButton);
        expect(await screen.findByRole("tooltip")).toHaveTextContent(licenseLimitWarning);
    });
});
