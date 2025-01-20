import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as stories from "./RevisionsBinCleaner.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import userEvent from "@testing-library/user-event";

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
        const user = userEvent.setup();
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const minimumEntriesAgeToKeepBefore = await screen.findByName("minimumEntriesAgeToKeepInMin");

        expect(minimumEntriesAgeToKeepBefore).toBeEnabled();
        expect(minimumEntriesAgeToKeepBefore).toHaveValue(
            DatabasesStubs.revisionsBinCleaner().MinimumEntriesAgeToKeepInMin
        );

        await user.click(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" }));

        const minimumEntriesAgeToKeepAfter = await screen.findByName("minimumEntriesAgeToKeepInMin");

        expect(minimumEntriesAgeToKeepAfter).toBeDisabled();
        expect(minimumEntriesAgeToKeepAfter).toHaveValue(null);
    });

    it("can disable 'set custom refresh frequency' after disabling 'enable revisions bin cleaner'", async () => {
        const user = userEvent.setup();
        const { screen } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const refreshFrequencyBefore = await screen.findByName("refreshFrequencyInSec");

        await user.click(screen.getByRole("checkbox", { name: "Set custom refresh frequency" }));

        expect(refreshFrequencyBefore).toBeEnabled();
        expect(refreshFrequencyBefore).toHaveValue(DatabasesStubs.revisionsBinCleaner().RefreshFrequencyInSec);

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
        const refreshFrequencySwitch = await screen.findByRole("checkbox", { name: "Set custom refresh frequency" });

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
            name: "Set custom refresh frequency",
        })) as HTMLInputElement;
        const refreshFrequencyBefore = await screen.findByName("refreshFrequencyInSec");

        expect(refreshFrequencyBefore).toHaveValue(500);
        expect(refreshFrequencySwitchBefore.checked).toBe(true);
        await user.click(refreshFrequencySwitchBefore);

        const refreshFrequencySwitchAfter = (await screen.findByRole("checkbox", {
            name: "Set custom refresh frequency",
        })) as HTMLInputElement;
        const refreshFrequencyAfter = await screen.findByName("refreshFrequencyInSec");

        expect(refreshFrequencyAfter).toHaveValue(null);
        expect(refreshFrequencyAfter).toBeDisabled();
        expect(refreshFrequencySwitchAfter.checked).toBe(false);
    });

    it("can remove value from 'set minimum entries age to keep' when disabling checkbox", async () => {
        const minimumEntriesAge = 3;

        const user = userEvent.setup();

        const { screen } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner
                revisionsBinCleanerDto={{
                    Disabled: false,
                    MinimumEntriesAgeToKeepInMin: minimumEntriesAge,
                    RefreshFrequencyInSec: 500,
                }}
            />
        );

        const setMinimumEntriesAgeToKeepSwitchBefore = (await screen.findByRole("checkbox", {
            name: "Set minimum entries age to keep",
        })) as HTMLInputElement;
        const setMinimumEntriesAgeToKeepBefore = await screen.findByName("minimumEntriesAgeToKeepInMin");

        expect(setMinimumEntriesAgeToKeepBefore).toHaveValue(minimumEntriesAge);
        expect(setMinimumEntriesAgeToKeepSwitchBefore.checked).toBe(true);

        await user.click(setMinimumEntriesAgeToKeepSwitchBefore);

        const setMinimumEntriesAgeToKeepSwitchAfter = (await screen.findByRole("checkbox", {
            name: "Set minimum entries age to keep",
        })) as HTMLInputElement;
        const setMinimumEntriesAgeToKeepAfter = await screen.findByName("minimumEntriesAgeToKeepInMin");

        expect(setMinimumEntriesAgeToKeepAfter).toHaveValue(null);
        expect(setMinimumEntriesAgeToKeepAfter).toBeDisabled();
        expect(setMinimumEntriesAgeToKeepSwitchAfter.checked).toBe(false);
    });
});
