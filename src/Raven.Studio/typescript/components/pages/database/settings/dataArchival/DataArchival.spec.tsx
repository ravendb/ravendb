import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DataArchival.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { LicenseAllowed, LicenseRestricted } = composeStories(stories);

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
});
