import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as stories from "./RevisionsBinCleaner.stories";
import userEvent from "@testing-library/user-event";
import { queryAllByClassName } from "test/byClassNameQueries";

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
        const user = userEvent.setup();

        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

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

        durationPickerInputsAfter.forEach((input, index) => {
            expect(input).toHaveValue(durationPickerValues[index]);
            expect(input).toBeDisabled();
        });
    });

    it("can disable 'set custom refresh frequency' after disabling 'enable revisions bin cleaner'", async () => {
        const user = userEvent.setup();
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const refreshFrequencyBefore = await screen.findByName("refreshFrequencyInSec");

        await user.click(screen.getByRole("checkbox", { name: "Set custom cleaner frequency" }));

        expect(refreshFrequencyBefore).toBeEnabled();

        await user.click(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" }));

        const refreshFrequencyAfter = await screen.findByName("refreshFrequencyInSec");

        expect(refreshFrequencyAfter).toBeDisabled();
        expect(refreshFrequencyAfter).toHaveValue(null);
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
        const refreshFrequencySwitch = await screen.findByRole("checkbox", { name: "Set custom cleaner frequency" });

        expect(isRevisionBinCleanerEnabledSwitch).toBeDisabled();
        expect(minimumEntriesAgeToKeepSwitch).toBeDisabled();
        expect(refreshFrequencySwitch).toBeDisabled();
    });

    it("can remove value from 'refresh frequency' when disabling checkbox", async () => {
        const user = userEvent.setup();
        const { screen } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner
                revisionsBinCleanerDto={{
                    Disabled: false,
                    MinimumEntriesAgeToKeepInMin: null,
                    RefreshFrequencyInSec: 500,
                }}
            />
        );

        const refreshFrequencySwitchBefore = (await screen.findByRole("checkbox", {
            name: "Set custom cleaner frequency",
        })) as HTMLInputElement;
        const refreshFrequencyBefore = await screen.findByName("refreshFrequencyInSec");

        expect(refreshFrequencyBefore).toHaveValue(500);
        expect(refreshFrequencySwitchBefore.checked).toBe(true);
        await user.click(refreshFrequencySwitchBefore);

        const refreshFrequencySwitchAfter = (await screen.findByRole("checkbox", {
            name: "Set custom cleaner frequency",
        })) as HTMLInputElement;
        const refreshFrequencyAfter = await screen.findByName("refreshFrequencyInSec");

        expect(refreshFrequencyAfter).toHaveValue(null);
        expect(refreshFrequencyAfter).toBeDisabled();
        expect(refreshFrequencySwitchAfter.checked).toBe(false);
    });
});
