import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DataArchival.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { LicenseAllowed, LicenseRestricted, InitialDataArchival } = composeStories(stories);

describe("DataArchival", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<LicenseAllowed />);

        expect(await screen.findByText("Enable Data Archival")).toBeInTheDocument();
    });

    it("can disable and set to null expiration frequency after disabling 'Enable Data Archival'", async () => {
        const { screen, fireClick } = rtlRender(<LicenseAllowed />);

        const archiveFrequencyBefore = await screen.findByName("archiveFrequency");
        expect(archiveFrequencyBefore).toBeEnabled();
        expect(archiveFrequencyBefore).toHaveValue(DatabasesStubs.dataArchivalConfiguration().ArchiveFrequencyInSec);

        await fireClick(screen.getByRole("checkbox", { name: "Enable Data Archival" }));

        const archiveFrequencyAfter = screen.getByName("archiveFrequency");
        expect(archiveFrequencyAfter).toBeDisabled();
        expect(archiveFrequencyAfter).toHaveValue(null);
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });

    it("can set default batch size", async () => {
        const { screen, fireClick } = rtlRender(<InitialDataArchival />);
        const enableButton = await screen.findByRole("checkbox", { name: "Enable Data Archival" });

        expect(enableButton).not.toBeChecked();

        await fireClick(enableButton);

        const setMaxNumberOfDocumentToProcessCheckbox = await screen.findByLabelText(
            "Set max number of documents to process in a single run"
        );
        expect(setMaxNumberOfDocumentToProcessCheckbox).toBeChecked();
        expect(await screen.findByName("maxItemsToProcess")).toHaveValue(65536);
    });
});
