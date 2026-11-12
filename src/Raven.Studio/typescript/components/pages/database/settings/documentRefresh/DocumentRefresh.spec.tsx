import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import appUrl from "common/appUrl";
import * as stories from "./DocumentRefresh.stories";

const { DefaultDocumentRefresh, LicenseRestricted, InitialDocumentRefresh } = composeStories(stories);

const licenseLimitWarning = "Your license doesn't allow a refresh frequency below 36 hours.";

describe("DocumentRefresh", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultDocumentRefresh />);

        expect(await screen.findByText("Enable Document Refresh")).toBeInTheDocument();
    });

    it("can disable and set to null refresh frequency after disabling 'Enable Document Refresh'", async () => {
        const { screen, fireClick } = rtlRender(<DefaultDocumentRefresh />);

        const refreshFrequency = await screen.findByRole("textbox", { name: "Refresh frequency" });
        expect(refreshFrequency).toBeEnabled();
        expect(refreshFrequency).toHaveValue("35h 59m 59s");

        await fireClick(screen.getByRole("checkbox", { name: "Enable Document Refresh" }));

        expect(refreshFrequency).toBeDisabled();
        expect(refreshFrequency).toHaveValue("");
    });

    it("pre-fills the optimal batch size and suggests it again after a reset", async () => {
        const { screen, fireClick } = rtlRender(<InitialDocumentRefresh />);
        const enableButton = await screen.findByRole("checkbox", { name: "Enable Document Refresh" });

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

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });

    it("shows the server default refresh frequency after a reset", async () => {
        const { screen, fireClick } = rtlRender(<DefaultDocumentRefresh />);

        await fireClick(
            await screen.findByRole("button", { name: "Reset to default", description: "Refresh frequency" })
        );

        expect(screen.getByRole("textbox", { name: "Refresh frequency" })).toHaveAttribute(
            "placeholder",
            "Default (1m)"
        );
    });

    it("shows the license limit while the frequency is below the license minimum", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByRole("textbox", { name: "Refresh frequency" })).toHaveValue("35h 59m 59s");
        expect(screen.getByText(licenseLimitWarning)).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "See full comparison" })).toHaveAttribute("href", appUrl.forAbout());
    });

    it("does not offer a reset to default on a limited license", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByRole("textbox", { name: "Refresh frequency" })).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Reset to default" })).not.toBeInTheDocument();
    });

    it("pre-fills the license minimum when Document Refresh is turned on", async () => {
        const { screen, fireClick } = rtlRender(<LicenseRestricted />);

        const documentRefreshSwitch = await screen.findByRole("checkbox", { name: "Enable Document Refresh" });
        const refreshFrequency = screen.getByRole("textbox", { name: "Refresh frequency" });

        await fireClick(documentRefreshSwitch);
        expect(refreshFrequency).toHaveValue("");

        await fireClick(documentRefreshSwitch);
        expect(refreshFrequency).toHaveValue("36h");
        expect(screen.getByText(/Minimum on your license/)).toHaveTextContent("Minimum on your license: 36 hours.");
        expect(screen.queryByText(licenseLimitWarning)).not.toBeInTheDocument();
    });

    it("disables the plan comparison link while Document Refresh is turned off", async () => {
        const { screen, fireClick } = rtlRender(<LicenseRestricted />);

        const documentRefreshSwitch = await screen.findByRole("checkbox", { name: "Enable Document Refresh" });
        const comparisonLink = screen.getByRole("link", { name: "See full comparison" });
        expect(comparisonLink).not.toHaveAttribute("aria-disabled", "true");

        await fireClick(documentRefreshSwitch);
        expect(comparisonLink).toHaveAttribute("aria-disabled", "true");
        expect(comparisonLink).toHaveAttribute("tabindex", "-1");

        await fireClick(documentRefreshSwitch);
        expect(comparisonLink).not.toHaveAttribute("aria-disabled", "true");
        expect(comparisonLink).not.toHaveAttribute("tabindex");
    });

    it("blocks saving while the frequency is below the license minimum", async () => {
        const { screen, user } = rtlRender(<LicenseRestricted />);

        const saveButton = await screen.findByRole("button", { name: "Save" });
        expect(saveButton).toBeDisabled();

        await user.hover(saveButton);
        expect(await screen.findByRole("tooltip")).toHaveTextContent(licenseLimitWarning);
    });
});
