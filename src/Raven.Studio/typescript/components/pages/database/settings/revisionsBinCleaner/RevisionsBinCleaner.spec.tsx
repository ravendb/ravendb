import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as stories from "./RevisionsBinCleaner.stories";
import { queryAllByClassName } from "test/byClassNameQueries";
import { within } from "@testing-library/dom";

const { DefaultRevisionsBinCleaner } = composeStories(stories);

describe("RevisionsBinCleaner", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultRevisionsBinCleaner />);

        const revisionBinCleanerHeading = await screen.findByRole("heading", {
            name: "Revisions Bin Cleaner",
        });
        expect(revisionBinCleanerHeading).toBeInTheDocument();
    });

    it("can disable 'set minimum entries age to keep' after disabling 'enable revisions bin cleaner'", async () => {
        const durationPickerValues = [0, 0, 1];

        const { screen, user } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const durationPickerBefore = await screen.findByTestId("durationPicker");
        const durationPickerInputsBefore = queryAllByClassName(durationPickerBefore, "form-control");
        expect(durationPickerInputsBefore).toHaveLength(3);

        expect(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" })).toBeChecked();

        durationPickerInputsBefore.forEach((input, index) => {
            expect(input).toHaveValue(durationPickerValues[index]);
            expect(input).not.toBeDisabled();
        });

        await user.click(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" }));

        expect(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" })).not.toBeChecked();

        const durationPickerAfter = await screen.findByTestId("durationPicker");
        const durationPickerInputsAfter = queryAllByClassName(durationPickerAfter, "form-control");
        expect(durationPickerInputsAfter).toHaveLength(3);

        durationPickerInputsAfter.forEach((input) => {
            expect(input).toHaveValue(null);
            expect(input).toBeDisabled();
        });
    });

    it("asks for an age greater than 0 when the minimum age is cleared", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const durationPicker = await screen.findByTestId("durationPicker");
        const [, , minutesInput] = queryAllByClassName(durationPicker, "form-control");

        await user.clear(minutesInput);

        expect(
            await screen.findByText(
                "The age must be greater than 0. To delete all items in the Revisions Bin, toggle 'Set minimum entries age to keep' off."
            )
        ).toBeInTheDocument();
    });

    it("can disable cleaner frequency after disabling 'enable revisions bin cleaner'", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const cleanerFrequency = await screen.findByRole("group", { name: "Cleaner frequency" });
        const seconds = within(cleanerFrequency).getByRole("spinbutton", { name: "Seconds" });
        expect(seconds).toBeEnabled();

        await user.type(seconds, "500");
        await user.click(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" }));

        expect(seconds).toBeDisabled();
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Cleaner frequency" })
        ).not.toBeInTheDocument();
    });

    it("can hide 'Save' button when has access below database admin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner databaseAccess="DatabaseReadWrite" />
        );

        expect(screen.queryByRole("button", { name: "Save" })).not.toBeInTheDocument();
    });

    it("can show 'Save' button when has database admin access", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner databaseAccess="DatabaseAdmin" />
        );

        expect(await screen.findByRole("button", { name: "Save" })).toBeInTheDocument();
    });

    it("can't click any switch when has access below database admin", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner databaseAccess="DatabaseReadWrite" />
        );

        const isRevisionBinCleanerEnabledSwitch = await screen.findByRole("checkbox", {
            name: "Enable Revisions Bin Cleaner",
        });
        const minimumEntriesAgeToKeepSwitch = await screen.findByRole("checkbox", {
            name: "Set minimum entries age to keep",
        });
        expect(isRevisionBinCleanerEnabledSwitch).toBeDisabled();
        expect(minimumEntriesAgeToKeepSwitch).toBeDisabled();
        const cleanerFrequency = await screen.findByRole("group", { name: "Cleaner frequency" });
        within(cleanerFrequency)
            .getAllByRole("spinbutton")
            .forEach((part) => expect(part).toBeDisabled());
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Cleaner frequency" })
        ).not.toBeInTheDocument();
    });

    it("can reset cleaner frequency to the default", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner
                revisionsBinCleanerDto={{
                    Disabled: false,
                    MinimumEntriesAgeToKeepInMin: null,
                    CleanerFrequencyInSec: 500,
                }}
            />
        );

        const cleanerFrequency = await screen.findByRole("group", { name: "Cleaner frequency" });
        const minutes = within(cleanerFrequency).getByRole("spinbutton", { name: "Minutes" });
        const seconds = within(cleanerFrequency).getByRole("spinbutton", { name: "Seconds" });
        expect(minutes).toHaveValue(8);
        expect(seconds).toHaveValue(20);

        await user.click(screen.getByRole("button", { name: "Reset to default", description: "Cleaner frequency" }));

        expect(minutes).toHaveValue(null);
        expect(minutes).toBeEnabled();
        expect(minutes).toHaveAttribute("placeholder", "Default (5)");
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Cleaner frequency" })
        ).not.toBeInTheDocument();
    });

    it("rejects a cleaner frequency of zero", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const cleanerFrequency = await screen.findByRole("group", { name: "Cleaner frequency" });
        await user.type(within(cleanerFrequency).getByRole("spinbutton", { name: "Seconds" }), "0");

        expect(await screen.findByText("Please enter positive number")).toBeInTheDocument();
    });
});
