import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentExpiration.stories";
import { queryAllByClassName } from "test/byClassNameQueries";

const { DefaultDocumentExpiration, LicenseRestricted, InitialDocumentExpiration } = composeStories(stories);

// DatabasesStubs.expirationConfiguration().DeleteFrequencyInSec is 129599 seconds
const expectedDeleteFrequency = [35, 59, 59];
const licenseLimitWarning = "Your license doesn't allow an expiration frequency below 36 hours.";
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

        const inputs = getDeleteFrequencyInputs(screen.getByTestId("deleteFrequencyDurationPicker"));
        inputs.forEach((input) => {
            expect(input).toBeEnabled();
            expect(input).toHaveValue(null);
        });
        expect(inputs.map((input) => input.getAttribute("placeholder"))).toEqual([
            "Default (0)",
            "Default (1)",
            "Default (0)",
        ]);
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Expiration frequency" })
        ).not.toBeInTheDocument();
    });

    it("shows zero instead of the default in empty parts once the frequency is overridden", async () => {
        const { screen, fireClick, user } = rtlRender(<InitialDocumentExpiration />);

        await fireClick(await screen.findByRole("checkbox", { name: "Enable Document Expiration" }));

        const [hours, minutes, seconds] = getDeleteFrequencyInputs(screen.getByTestId("deleteFrequencyDurationPicker"));
        await user.type(hours, "123");

        expect(minutes).toHaveValue(null);
        expect(minutes).toHaveAttribute("placeholder", "0");
        expect(seconds).toHaveAttribute("placeholder", "0");
    });

    it("is license restricted", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByText(/Licensing/)).toBeInTheDocument();
    });

    it("shows the license limit while the frequency is below the license minimum", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        const deleteFrequency = getDeleteFrequencyInputs(await screen.findByTestId("deleteFrequencyDurationPicker"));
        deleteFrequency.forEach((input, index) => {
            expect(input).toBeEnabled();
            expect(input).toHaveValue(expectedDeleteFrequency[index]);
        });

        expect(screen.getByText(licenseLimitWarning)).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "Compare license plans" })).toBeInTheDocument();
    });

    it("does not offer a reset to default on a limited license", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByRole("group", { name: "Expiration frequency" })).toBeInTheDocument();
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Expiration frequency" })
        ).not.toBeInTheDocument();
    });

    it("pre-fills the license minimum when Document Expiration is turned on", async () => {
        const { screen, fireClick } = rtlRender(<LicenseRestricted />);

        const documentExpirationSwitch = await screen.findByRole("checkbox", { name: "Enable Document Expiration" });

        await fireClick(documentExpirationSwitch);
        getDeleteFrequencyInputs(screen.getByTestId("deleteFrequencyDurationPicker")).forEach((input) => {
            expect(input).toHaveValue(null);
        });

        await fireClick(documentExpirationSwitch);

        const deleteFrequency = getDeleteFrequencyInputs(screen.getByTestId("deleteFrequencyDurationPicker"));
        deleteFrequency.forEach((input, index) => {
            expect(input).toHaveValue(expectedLicenseMinimumDeleteFrequency[index]);
        });
        expect(screen.getByText(/Minimum on your license/)).toHaveTextContent("Minimum on your license: 36 hours.");
        expect(screen.queryByText(licenseLimitWarning)).not.toBeInTheDocument();
    });

    it("blocks saving while the frequency is below the license minimum", async () => {
        const { screen, user } = rtlRender(<LicenseRestricted />);

        const saveButton = await screen.findByRole("button", { name: "Save" });
        expect(saveButton).toBeDisabled();

        await user.hover(saveButton);
        expect(await screen.findByRole("tooltip")).toHaveTextContent(licenseLimitWarning);
    });
});
