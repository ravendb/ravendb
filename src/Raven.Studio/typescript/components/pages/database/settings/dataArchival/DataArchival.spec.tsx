import React from "react";
import { composeStories } from "@storybook/react-webpack5";
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

    it("pre-fills the optimal batch size and suggests it again after a reset", async () => {
        const { screen, fireClick } = rtlRender(<InitialDataArchival />);
        const enableButton = await screen.findByRole("checkbox", { name: "Enable Data Archival" });

        expect(enableButton).not.toBeChecked();
        expect(screen.getByName("maxItemsToProcess")).toHaveAttribute("placeholder", "Default (unlimited)");

        await fireClick(enableButton);

        expect(await screen.findByName("maxItemsToProcess")).toHaveValue(65536);
        expect(
            screen.queryByText("Process at most 65,536 documents per run to keep each run short.")
        ).not.toBeInTheDocument();
        expect(screen.getByText("Optimal", { selector: ".badge" })).toBeInTheDocument();

        await fireClick(
            screen.getByRole("button", {
                name: "Reset to default",
                description: "Max number of documents to process in a single run",
            })
        );

        expect(screen.getByName("maxItemsToProcess")).toHaveValue(null);
        expect(
            screen.getByText("Process at most 65,536 documents per run to keep each run short.")
        ).toBeInTheDocument();
    });
});
