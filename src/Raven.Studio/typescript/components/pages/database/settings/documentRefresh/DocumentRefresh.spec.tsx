import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { within } from "@testing-library/dom";
import { rtlRender } from "test/rtlTestUtils";
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

        const durationPicker = await screen.findByTestId("refreshFrequencyDurationPicker");
        const hoursInput = within(durationPicker).getByRole("spinbutton", { name: "Hours" });
        expect(hoursInput).toBeEnabled();
        expect(hoursInput).toHaveValue(35);

        await fireClick(screen.getByRole("checkbox", { name: "Enable Document Refresh" }));

        expect(hoursInput).toBeDisabled();
        expect(hoursInput).toHaveValue(null);
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

        const durationPicker = screen.getByTestId("refreshFrequencyDurationPicker");
        expect(within(durationPicker).getByRole("spinbutton", { name: "Hours" })).toHaveAttribute(
            "placeholder",
            "Default (0)"
        );
        expect(within(durationPicker).getByRole("spinbutton", { name: "Minutes" })).toHaveAttribute(
            "placeholder",
            "Default (1)"
        );
        expect(within(durationPicker).getByRole("spinbutton", { name: "Seconds" })).toHaveAttribute(
            "placeholder",
            "Default (0)"
        );
    });

    it("shows the license limit while the frequency is below the license minimum", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        const durationPicker = await screen.findByTestId("refreshFrequencyDurationPicker");

        expect(within(durationPicker).getByRole("spinbutton", { name: "Hours" })).toHaveValue(35);
        expect(within(durationPicker).getByRole("spinbutton", { name: "Minutes" })).toHaveValue(59);
        expect(within(durationPicker).getByRole("spinbutton", { name: "Seconds" })).toHaveValue(59);
        expect(screen.getByText(licenseLimitWarning)).toBeInTheDocument();
        expect(screen.getByRole("link", { name: "Compare license plans" })).toBeInTheDocument();
    });

    it("does not offer a reset to default on a limited license", async () => {
        const { screen } = rtlRender(<LicenseRestricted />);

        expect(await screen.findByRole("group", { name: "Refresh frequency" })).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Reset to default" })).not.toBeInTheDocument();
    });

    it("pre-fills the license minimum when Document Refresh is turned on", async () => {
        const { screen, fireClick } = rtlRender(<LicenseRestricted />);

        const documentRefreshSwitch = await screen.findByRole("checkbox", { name: "Enable Document Refresh" });
        const hoursInput = within(screen.getByTestId("refreshFrequencyDurationPicker")).getByRole("spinbutton", {
            name: "Hours",
        });

        await fireClick(documentRefreshSwitch);
        expect(hoursInput).toHaveValue(null);

        await fireClick(documentRefreshSwitch);
        expect(hoursInput).toHaveValue(36);
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
