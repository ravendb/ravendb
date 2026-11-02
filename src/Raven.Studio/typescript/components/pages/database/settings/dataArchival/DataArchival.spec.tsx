import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DataArchival.stories";
import { within } from "@testing-library/dom";

const { LicenseAllowed, LicenseRestricted, InitialDataArchival } = composeStories(stories);

describe("DataArchival", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<LicenseAllowed />);

        expect(await screen.findByText("Enable Data Archival")).toBeInTheDocument();
    });

    it("can disable and set to null expiration frequency after disabling 'Enable Data Archival'", async () => {
        const { screen, fireClick } = rtlRender(<LicenseAllowed />);

        const archiveFrequency = await screen.findByRole("group", { name: "Archive frequency" });
        const minutes = within(archiveFrequency).getByRole("spinbutton", { name: "Minutes" });
        const seconds = within(archiveFrequency).getByRole("spinbutton", { name: "Seconds" });
        expect(minutes).toBeEnabled();
        expect(minutes).toHaveValue(1);
        expect(seconds).toHaveValue(5);

        await fireClick(screen.getByRole("checkbox", { name: "Enable Data Archival" }));

        expect(minutes).toBeDisabled();
        expect(minutes).toHaveValue(null);
        expect(seconds).toHaveValue(null);
    });

    it("rejects an archive frequency of zero", async () => {
        const { screen, user } = rtlRender(<LicenseAllowed />);

        const archiveFrequency = await screen.findByRole("group", { name: "Archive frequency" });
        for (const part of within(archiveFrequency).getAllByRole("spinbutton")) {
            await user.clear(part);
            await user.type(part, "0");
        }

        expect(await screen.findByText(/positive number/)).toBeInTheDocument();
    });

    it("goes back to the default archive frequency when every part is cleared", async () => {
        const { screen, user } = rtlRender(<LicenseAllowed />);

        const archiveFrequency = await screen.findByRole("group", { name: "Archive frequency" });
        for (const part of within(archiveFrequency).getAllByRole("spinbutton")) {
            await user.clear(part);
        }

        expect(within(archiveFrequency).getByRole("spinbutton", { name: "Minutes" })).toHaveAttribute(
            "placeholder",
            "Default (1)"
        );
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
