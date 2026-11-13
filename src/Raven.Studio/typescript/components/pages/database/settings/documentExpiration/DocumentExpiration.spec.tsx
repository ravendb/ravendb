import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import appUrl from "common/appUrl";
import * as stories from "./DocumentExpiration.stories";

const { DefaultDocumentExpiration, LicenseRestricted, InitialDocumentExpiration } = composeStories(stories);

// DatabasesStubs.expirationConfiguration().DeleteFrequencyInSec is 129599 seconds
const expectedDeleteFrequency = "35h 59m 59s";
const licenseLimitWarning = "Your license doesn't allow an expiration frequency below 36 hours.";
// LicenseStubs limits the minimum period to 36 hours
const expectedLicenseMinimumDeleteFrequency = "36h";

describe("DocumentExpiration", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultDocumentExpiration />);

        expect(await screen.findByText("Enable Document Expiration")).toBeInTheDocument();
    });

    it("can disable expiration frequency after disabling 'Enable Document Expiration'", async () => {
        const { screen, fireClick } = rtlRender(<DefaultDocumentExpiration />);

        const deleteFrequency = await screen.findByRole("textbox", { name: "Expiration frequency" });
        expect(deleteFrequency).toBeEnabled();
        expect(deleteFrequency).toHaveValue(expectedDeleteFrequency);

        await fireClick(screen.getByRole("checkbox", { name: "Enable Document Expiration" }));

        expect(deleteFrequency).toBeDisabled();
        expect(deleteFrequency).toHaveValue("");
    });

    it("pre-fills the optimal batch size and suggests it again after a reset", async () => {
        const { screen, fireClick } = rtlRender(<InitialDocumentExpiration />);
        const enableButton = await screen.findByRole("checkbox", { name: "Enable Document Expiration" });

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

    it("shows the default expiration frequency in an empty, editable field", async () => {
        const { screen, fireClick } = rtlRender(<InitialDocumentExpiration />);

        await fireClick(await screen.findByRole("checkbox", { name: "Enable Document Expiration" }));

        const deleteFrequency = screen.getByRole("textbox", { name: "Expiration frequency" });
        expect(deleteFrequency).toBeEnabled();
        expect(deleteFrequency).toHaveValue("");
        expect(deleteFrequency).toHaveAttribute("placeholder", "Default (1m)");
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Expiration frequency" })
        ).not.toBeInTheDocument();
    });

    it("offers a reset to default once the frequency is overridden", async () => {
        const { screen, fireClick, user } = rtlRender(<InitialDocumentExpiration />);

        await fireClick(await screen.findByRole("checkbox", { name: "Enable Document Expiration" }));

        const deleteFrequency = screen.getByRole("textbox", { name: "Expiration frequency" });
        await user.type(deleteFrequency, "123h{Enter}");

        expect(deleteFrequency).toHaveValue("123h");
        expect(
            screen.getByRole("button", { name: "Reset to default", description: "Expiration frequency" })
        ).toBeInTheDocument();
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });

    it("shows the license limit while the frequency is below the license minimum", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        const deleteFrequency = await screen.findByRole("textbox", { name: "Expiration frequency" });
        expect(deleteFrequency).toBeEnabled();
        expect(deleteFrequency).toHaveValue(expectedDeleteFrequency);

        expect(screen.getByText(licenseLimitWarning)).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "See full comparison" })).toHaveAttribute("href", appUrl.forAbout());
    });

    it("does not offer a reset to default on a limited license", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByRole("textbox", { name: "Expiration frequency" })).toBeInTheDocument();
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Expiration frequency" })
        ).not.toBeInTheDocument();
    });

    it("pre-fills the license minimum when Document Expiration is turned on", async () => {
        const { screen, fireClick } = rtlRender(<LicenseRestricted />);

        const documentExpirationSwitch = await screen.findByRole("checkbox", { name: "Enable Document Expiration" });

        const deleteFrequency = screen.getByRole("textbox", { name: "Expiration frequency" });

        await fireClick(documentExpirationSwitch);
        expect(deleteFrequency).toHaveValue("");

        await fireClick(documentExpirationSwitch);
        expect(deleteFrequency).toHaveValue(expectedLicenseMinimumDeleteFrequency);
        expect(screen.getByText(/Minimum on your license/)).toHaveTextContent("Minimum on your license: 36 hours.");
        expect(screen.queryByText(licenseLimitWarning)).not.toBeInTheDocument();
    });

    it("disables the plan comparison link while Document Expiration is turned off", async () => {
        const { screen, fireClick } = rtlRender(<LicenseRestricted />);

        const documentExpirationSwitch = await screen.findByRole("checkbox", { name: "Enable Document Expiration" });
        const comparisonLink = screen.getByRole("link", { name: "See full comparison" });
        expect(comparisonLink).not.toHaveAttribute("aria-disabled", "true");

        await fireClick(documentExpirationSwitch);
        expect(comparisonLink).toHaveAttribute("aria-disabled", "true");
        expect(comparisonLink).toHaveAttribute("tabindex", "-1");

        await fireClick(documentExpirationSwitch);
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
