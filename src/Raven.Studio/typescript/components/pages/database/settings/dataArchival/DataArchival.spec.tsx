import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DataArchival.stories";

const { LicenseAllowed, LicenseRestricted, InitialDataArchival } = composeStories(stories);

describe("DataArchival", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<LicenseAllowed />);

        expect(await screen.findByText("Enable Data Archival")).toBeInTheDocument();
    });

    it("can disable and set to null expiration frequency after disabling 'Enable Data Archival'", async () => {
        const { screen, fireClick } = rtlRender(<LicenseAllowed />);

        const archiveFrequency = await screen.findByRole("textbox", { name: "Archive frequency" });
        expect(archiveFrequency).toBeEnabled();
        expect(archiveFrequency).toHaveValue("1m 5s");

        await fireClick(screen.getByRole("checkbox", { name: "Enable Data Archival" }));

        expect(archiveFrequency).toBeDisabled();
        expect(archiveFrequency).toHaveValue("");
    });

    it("rejects an archive frequency of zero", async () => {
        const { screen, user } = rtlRender(<LicenseAllowed />);

        const archiveFrequency = await screen.findByRole("textbox", { name: "Archive frequency" });
        await user.clear(archiveFrequency);
        await user.type(archiveFrequency, "0{Enter}");

        expect(await screen.findByText(/positive number/)).toBeInTheDocument();
    });

    it("goes back to the default archive frequency when the frequency is cleared", async () => {
        const { screen, user } = rtlRender(<LicenseAllowed />);

        const archiveFrequency = await screen.findByRole("textbox", { name: "Archive frequency" });
        await user.clear(archiveFrequency);
        await user.keyboard("{Enter}");

        expect(archiveFrequency).toHaveAttribute("placeholder", "Default (1m)");
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Archive frequency" })
        ).not.toBeInTheDocument();
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
