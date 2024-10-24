import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentExpiration.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DefaultDocumentExpiration, LicenseRestricted, InitialDocumentExpiration } = composeStories(stories);

describe("DocumentExpiration", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultDocumentExpiration />);

        expect(await screen.findByText("Enable Document Expiration")).toBeInTheDocument();
    });

    it("can disable and set to null expiration frequency after disabling 'Enable Document Expiration'", async () => {
        const { screen, fireClick } = rtlRender(<DefaultDocumentExpiration />);

        const deleteFrequencyBefore = await screen.findByName("deleteFrequency");
        expect(deleteFrequencyBefore).toBeEnabled();
        expect(deleteFrequencyBefore).toHaveValue(DatabasesStubs.expirationConfiguration().DeleteFrequencyInSec);

        await fireClick(screen.getByRole("checkbox", { name: "Enable Document Expiration" }));

        const deleteFrequencyAfter = screen.getByName("deleteFrequency");
        expect(deleteFrequencyAfter).toBeDisabled();
        expect(deleteFrequencyAfter).toHaveValue(null);
    });

    it("can set default batch size", async () => {
        const { screen, fireClick } = rtlRender(<InitialDocumentExpiration />);
        const enableButton = await screen.findByRole("checkbox", { name: "Enable Document Expiration" });

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

        const customExpirationFrequency = await screen.findByName("deleteFrequency");
        expect(customExpirationFrequency).toBeEnabled();
        expect(customExpirationFrequency).toHaveValue(DatabasesStubs.expirationConfiguration().DeleteFrequencyInSec);

        const isAlertVisible = screen.getByText(
            "Your current license does not allow a frequency higher than 36 hours (129600 seconds)"
        );
        expect(isAlertVisible).toBeInTheDocument();
    });
});
