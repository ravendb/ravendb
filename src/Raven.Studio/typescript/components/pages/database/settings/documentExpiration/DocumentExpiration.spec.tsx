import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentExpiration.stories";
import { queryAllByClassName } from "test/byClassNameQueries";

const { DefaultDocumentExpiration, LicenseRestricted, InitialDocumentExpiration } = composeStories(stories);

// DatabasesStubs.expirationConfiguration().DeleteFrequencyInSec is 129599 seconds
const expectedDeleteFrequency = [35, 59, 59];
const licenseLimitWarning = "Your current license does not allow an expiration frequency below 36 hours.";
// LicenseStubs limits the minimum period to 36 hours
const expectedLicenseMinimumDeleteFrequency = [36, 0, 0];

function getDeleteFrequencyInputs(durationPicker: HTMLElement) {
    const inputs = queryAllByClassName(durationPicker, "form-control");
    expect(inputs).toHaveLength(3);

    return inputs;
}

describe("DocumentExpiration", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultDocumentExpiration />);

        expect(await screen.findByText("Enable Document Expiration")).toBeInTheDocument();
    });

    it("can disable expiration frequency after disabling 'Enable Document Expiration'", async () => {
        const { screen, fireClick } = rtlRender(<DefaultDocumentExpiration />);

        const deleteFrequencyBefore = getDeleteFrequencyInputs(
            await screen.findByTestId("deleteFrequencyDurationPicker")
        );
        deleteFrequencyBefore.forEach((input, index) => {
            expect(input).toBeEnabled();
            expect(input).toHaveValue(expectedDeleteFrequency[index]);
        });

        await fireClick(screen.getByRole("checkbox", { name: "Enable Document Expiration" }));

        const deleteFrequencyAfter = getDeleteFrequencyInputs(screen.getByTestId("deleteFrequencyDurationPicker"));
        deleteFrequencyAfter.forEach((input) => {
            expect(input).toBeDisabled();
            expect(input).toHaveValue(null);
        });
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

        const customExpirationFrequency = getDeleteFrequencyInputs(
            await screen.findByTestId("deleteFrequencyDurationPicker")
        );
        customExpirationFrequency.forEach((input, index) => {
            expect(input).toBeEnabled();
            expect(input).toHaveValue(expectedDeleteFrequency[index]);
        });

        expect(screen.getByText(licenseLimitWarning)).toBeInTheDocument();
    });

    it("turns custom expiration frequency on when the license enforces a minimum period", async () => {
        const { screen, fireClick } = rtlRender(<LicenseRestricted />);

        const documentExpirationSwitch = await screen.findByRole("checkbox", { name: "Enable Document Expiration" });

        await fireClick(documentExpirationSwitch);
        expect(screen.getByRole("checkbox", { name: "Set custom expiration frequency" })).not.toBeChecked();

        await fireClick(documentExpirationSwitch);
        expect(screen.getByRole("checkbox", { name: "Set custom expiration frequency" })).toBeChecked();

        const deleteFrequency = getDeleteFrequencyInputs(screen.getByTestId("deleteFrequencyDurationPicker"));
        deleteFrequency.forEach((input, index) => {
            expect(input).toHaveValue(expectedLicenseMinimumDeleteFrequency[index]);
        });

        expect(screen.queryByText(licenseLimitWarning)).not.toBeInTheDocument();
    });

    it("blocks saving when the frequency falls back to the server default on a limited license", async () => {
        const { screen, fireClick } = rtlRender(<LicenseRestricted />);

        await fireClick(await screen.findByRole("checkbox", { name: "Set custom expiration frequency" }));

        expect(screen.getByRole("checkbox", { name: "Set custom expiration frequency" })).not.toBeChecked();
        expect(screen.getByText(licenseLimitWarning)).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();
    });
});
