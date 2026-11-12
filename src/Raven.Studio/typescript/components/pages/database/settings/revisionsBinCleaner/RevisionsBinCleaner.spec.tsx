import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { rtlRender, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as stories from "./RevisionsBinCleaner.stories";

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

        const minimumEntriesAge = await screen.findByRole("textbox", { name: "Minimum entries age" });

        expect(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" })).toBeChecked();
        expect(minimumEntriesAge).toHaveValue("1m");
        expect(minimumEntriesAge).toBeEnabled();

        await user.click(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" }));

        expect(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" })).not.toBeChecked();
        expect(minimumEntriesAge).toHaveValue("");
        expect(minimumEntriesAge).toBeDisabled();
    });

    it("rejects a minimum entries age of zero", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const minimumEntriesAge = await screen.findByRole("textbox", { name: "Minimum entries age" });
        await user.clear(minimumEntriesAge);
        await user.type(minimumEntriesAge, "0{Enter}");

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

        const minimumEntriesAge = await screen.findByRole("textbox", { name: "Minimum entries age" });
        expect(minimumEntriesAge).toHaveValue("");
        expect(minimumEntriesAge).toBeEnabled();
        expect(minimumEntriesAge).toHaveAttribute("placeholder", "Default (30d)");
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

        const minimumEntriesAge = await screen.findByRole("textbox", { name: "Minimum entries age" });
        expect(minimumEntriesAge).toHaveValue("");
        expect(minimumEntriesAge).toHaveAttribute("placeholder", "Default (30d)");
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

        const minimumEntriesAge = await screen.findByRole("textbox", { name: "Minimum entries age" });
        expect(minimumEntriesAge).toHaveValue("2h");

        await user.click(screen.getByRole("button", { name: "Reset to default", description: "Minimum entries age" }));

        expect(minimumEntriesAge).toHaveValue("");
        expect(minimumEntriesAge).toHaveAttribute("placeholder", "Default (30d)");
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Minimum entries age" })
        ).not.toBeInTheDocument();
    });

    it("can disable cleaner frequency after disabling 'enable revisions bin cleaner'", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        const cleanerFrequency = await screen.findByRole("textbox", { name: "Cleaner frequency" });
        expect(cleanerFrequency).toBeEnabled();

        await user.type(cleanerFrequency, "500{Enter}");
        await user.click(screen.getByRole("checkbox", { name: "Enable Revisions Bin Cleaner" }));

        expect(cleanerFrequency).toBeDisabled();
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
        expect(await screen.findByRole("textbox", { name: "Cleaner frequency" })).toBeDisabled();
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

        const cleanerFrequency = await screen.findByRole("textbox", { name: "Cleaner frequency" });
        expect(cleanerFrequency).toHaveValue("8m 20s");

        await user.click(screen.getByRole("button", { name: "Reset to default", description: "Cleaner frequency" }));

        expect(cleanerFrequency).toHaveValue("");
        expect(cleanerFrequency).toBeEnabled();
        expect(cleanerFrequency).toHaveAttribute("placeholder", "Default (5m)");
        expect(
            screen.queryByRole("button", { name: "Reset to default", description: "Cleaner frequency" })
        ).not.toBeInTheDocument();
    });

    it("rejects a cleaner frequency of zero", async () => {
        const { screen, user } = await rtlRender_WithWaitForLoad(<DefaultRevisionsBinCleaner />);

        await user.type(await screen.findByRole("textbox", { name: "Cleaner frequency" }), "0{Enter}");

        expect(await screen.findByText("Please enter positive number")).toBeInTheDocument();
    });
});
