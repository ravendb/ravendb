import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as stories from "./RevisionsBinCleaner.stories";
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
        const { screen, user } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const minimumEntriesAge = await screen.findByRole("group", { name: "Minimum entries age" });
        const parts = within(minimumEntriesAge).getAllByRole("spinbutton");
        expect(parts).toHaveLength(3);

        expect(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" })).toBeChecked();

        parts.forEach((part, index) => {
            expect(part).toHaveValue([0, 0, 1][index]);
            expect(part).toBeEnabled();
        });

        await user.click(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" }));

        expect(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" })).not.toBeChecked();

        parts.forEach((part) => {
            expect(part).toHaveValue(null);
            expect(part).toBeDisabled();
        });
    });

    it("rejects a minimum entries age of zero", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const minimumEntriesAge = await screen.findByRole("group", { name: "Minimum entries age" });
        await user.clear(within(minimumEntriesAge).getByRole("spinbutton", { name: "Minutes" }));

        expect(
            await screen.findByText(
                "The age must be greater than 0. To delete all items in the Revisions Bin, toggle 'Set minimum entries age to keep' off."
            )
        ).toBeInTheDocument();
    });

    it("shows the default minimum entries age after turning on 'set minimum entries age to keep'", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner
                revisionsBinCleanerDto={{
                    Disabled: false,
                    MinimumEntriesAgeToKeepInMin: 0,
                    CleanerFrequencyInSec: 300,
                }}
            />
        );

        await user.click(screen.getByRole("checkbox", { name: "Set minimum entries age to keep" }));

        const minimumEntriesAge = await screen.findByRole("group", { name: "Minimum entries age" });
        const days = within(minimumEntriesAge).getByRole("spinbutton", { name: "Days" });
        expect(days).toHaveValue(null);
        expect(days).toBeEnabled();
        expect(days).toHaveAttribute("placeholder", "Default (30)");
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Minimum entries age" })
        ).not.toBeInTheDocument();
    });

    it("shows a stored minimum entries age of 30 days as the default", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner
                revisionsBinCleanerDto={{
                    Disabled: false,
                    MinimumEntriesAgeToKeepInMin: 30 * 24 * 60,
                    CleanerFrequencyInSec: 300,
                }}
            />
        );

        const minimumEntriesAge = await screen.findByRole("group", { name: "Minimum entries age" });
        const days = within(minimumEntriesAge).getByRole("spinbutton", { name: "Days" });
        expect(days).toHaveValue(null);
        expect(days).toHaveAttribute("placeholder", "Default (30)");
    });

    it("can reset minimum entries age to the default", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner
                revisionsBinCleanerDto={{
                    Disabled: false,
                    MinimumEntriesAgeToKeepInMin: 120,
                    CleanerFrequencyInSec: 300,
                }}
            />
        );

        const minimumEntriesAge = await screen.findByRole("group", { name: "Minimum entries age" });
        const days = within(minimumEntriesAge).getByRole("spinbutton", { name: "Days" });
        const hours = within(minimumEntriesAge).getByRole("spinbutton", { name: "Hours" });
        expect(hours).toHaveValue(2);

        await user.click(screen.getByRole("button", { name: "Reset to default", description: "Minimum entries age" }));

        expect(hours).toHaveValue(null);
        expect(days).toHaveAttribute("placeholder", "Default (30)");
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Minimum entries age" })
        ).not.toBeInTheDocument();
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

    it.each(["DatabaseRead", "DatabaseReadWrite"] as const)(
        "disables 'Save' and explains the required access for %s",
        async (databaseAccess) => {
            const { screen, user } = await rtlRender_WithWaitForLoad(
                <DefaultRevisionsBinCleaner databaseAccess={databaseAccess} />
            );

            const saveButton = await screen.findByRole("button", { name: "Save" });
            expect(saveButton).toBeDisabled();

            await user.hover(saveButton);
            expect(await screen.findByRole("tooltip")).toHaveTextContent(
                "You don't have the required permissions (Database Admin access required)"
            );
        }
    );

    it("enables 'Save' for database admin after a change", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(
            <DefaultRevisionsBinCleaner databaseAccess="DatabaseAdmin" />
        );

        await user.click(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" }));

        expect(await screen.findByRole("button", { name: "Save" })).toBeEnabled();
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
