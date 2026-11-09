import moment from "moment";
import { waitFor, within } from "@testing-library/react";
import { composeStories } from "@storybook/react-webpack5";
import { mockServices } from "test/mocks/services/MockServices";
import { RtlScreen, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import * as stories from "./BackupDecisionLog.stories";

const { BackupDecisionLogStory } = composeStories(stories);

const entries = ManageServerStubs.backupDecisionLog().Entries;
const reasonOf = (kind: string) => entries.find((x) => x.Kind === kind).Reason;
const failedReason = reasonOf("Failed");
const completedReason = reasonOf("Completed");
const infoEntry = entries.find((x) => x.Kind === "Info");
const heldBackReason = entries.find((x) => x.Detail === "BackupDisabled").Reason;
const cardOf = (screen: RtlScreen, title: string) =>
    within(screen.getByText(title, { selector: ".card-body *" }).closest(".card-body") as HTMLElement);

describe("BackupDecisionLog", () => {
    it("renders decisions of the current node", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

        expect(mockServices.manageServerService.mock.getBackupDecisionLog).toHaveBeenCalledWith();
        expect(screen.getByText("Node A")).toBeInTheDocument();
        expect(screen.getByText(failedReason)).toBeInTheDocument();
        expect(screen.getByText(`Showing the newest ${entries.length} events`, { exact: false })).toBeInTheDocument();
    });

    it("shows empty state when there are no decisions", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <BackupDecisionLogStory
                details={(x) => {
                    x.Entries = [];
                }}
            />
        );

        expect(screen.getByText("No decisions yet")).toBeInTheDocument();
    });

    it("shows load error on first load failure and retries", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory isLoadFailing />);

        expect(screen.getByText(/Unable to load the backup decision log/)).toBeInTheDocument();

        mockServices.manageServerService.withBackupDecisionLog();
        await fireClick(screen.getByRole("button", { name: /Refresh/ }));

        expect(await screen.findByText(failedReason)).toBeInTheDocument();
    });

    it("renders collapsed repeats with range and count", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

        expect(screen.getAllByText("x 5")).toHaveLength(3);
        expect(screen.getAllByText(/\d [AP]M - \d/)).toHaveLength(3);
    });

    it("renders server-wide and shard markers", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

        expect(screen.getAllByText("Server-wide")).toHaveLength(2);
        expect(screen.getAllByText("$0")).toHaveLength(2);
    });

    it("shows queue and per-task state in cards", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

        expect(screen.getByText("1/4")).toBeInTheDocument();
        expect(screen.getByText("Up to 4 can run at once")).toBeInTheDocument();
        expect(screen.getByText(/^Next: task 'Daily backup' - \d\d:\d\d$/)).toBeInTheDocument();
        expect(cardOf(screen, "Waiting for schedule").getByText("2")).toBeInTheDocument();
        expect(cardOf(screen, "Held back").getByText("1")).toBeInTheDocument();
        expect(screen.getAllByText(heldBackReason)).toHaveLength(2);
        expect(screen.getByText(/^Failed - since \d\d:\d\d$/)).toBeInTheDocument();
        expect(screen.getByText(/^Latest: task 'Weekly full backup', \d\d:\d\d$/)).toBeInTheDocument();
    });

    describe("filters", () => {
        it("filters by event", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Event" }));
            expect(screen.getByRole("checkbox", { name: "Info" })).toBeInTheDocument();
            await fireClick(screen.getByRole("checkbox", { name: "Failed" }));

            expect(screen.getByRole("button", { name: "Event: Failed" })).toBeInTheDocument();
            expect(screen.getByText(failedReason)).toBeInTheDocument();
            expect(screen.queryByText(infoEntry.Reason)).not.toBeInTheDocument();
        });

        it("filters by reason text", async () => {
            const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Reason" }));
            await fillInput(screen.getByPlaceholderText("e.g. Task disabled"), "DESTINATION is unreachable");

            expect(
                await screen.findByRole("button", { name: "Reason: DESTINATION is unreachable" })
            ).toBeInTheDocument();
            expect(screen.getByText(failedReason)).toBeInTheDocument();
            expect(screen.queryByText(infoEntry.Reason)).not.toBeInTheDocument();
        });

        it("database filter loads each selected database and keeps server-wide rows", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);
            const getLogMock = mockServices.manageServerService.mock.getBackupDecisionLog;

            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Database" }));
            expect(screen.queryByRole("checkbox", { name: "Orders$0" })).not.toBeInTheDocument();
            await fireClick(screen.getByRole("checkbox", { name: "Invoices" }));

            expect(await screen.findByRole("button", { name: "Database: Invoices" })).toBeInTheDocument();
            expect(getLogMock).toHaveBeenCalledWith("Invoices");
            await waitFor(() => expect(screen.queryByText(failedReason)).not.toBeInTheDocument());
            expect(screen.getByText(/events for the selected databases/)).toBeInTheDocument();
            expect(screen.getByText(completedReason)).toBeInTheDocument();
            expect(screen.getAllByText("Server-wide")).toHaveLength(2);

            await fireClick(screen.getByRole("checkbox", { name: "Orders" }));

            expect(getLogMock).toHaveBeenCalledWith("Orders");
            expect(await screen.findByText(failedReason)).toBeInTheDocument();
            expect(
                screen.getByText(`Showing ${entries.length} events for the selected databases.`)
            ).toBeInTheDocument();
            expect(screen.getAllByText("Server-wide")).toHaveLength(2);
        });

        it("shows load error when a database filter request fails and retries", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            mockServices.manageServerService.withThrowingGetBackupDecisionLog();
            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Database" }));
            await fireClick(screen.getByRole("checkbox", { name: "Invoices" }));

            expect(await screen.findByText(/Unable to load the backup decision log/)).toBeInTheDocument();
            expect(screen.queryByText(failedReason)).not.toBeInTheDocument();

            mockServices.manageServerService.withBackupDecisionLog();
            await fireClick(screen.getByRole("button", { name: /Refresh/ }));

            expect(await screen.findByText(completedReason)).toBeInTheDocument();
            expect(screen.queryByText(failedReason)).not.toBeInTheDocument();
        });

        it("includes the whole selected To day", async () => {
            const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);
            const newestDay = moment(infoEntry.Time).format("DD/MM/YYYY");

            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Date range" }));
            await fillInput(screen.getAllByRole("textbox")[1], newestDay + " 00:00");

            expect(await screen.findByRole("button", { name: new RegExp(`- ${newestDay} 23:59`) })).toBeInTheDocument();
            expect(screen.getByText(infoEntry.Reason)).toBeInTheDocument();
        });

        it("clearing the reason chip closes the menu and empties the reason input", async () => {
            const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Reason" }));
            await fillInput(screen.getByPlaceholderText("e.g. Task disabled"), "no such reason");
            await screen.findByRole("button", { name: "Reason: no such reason" });

            await fireClick(screen.getByRole("button", { name: "Clear Reason filter" }));
            expect(screen.queryByPlaceholderText("e.g. Task disabled")).not.toBeInTheDocument();

            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Reason" }));
            expect(screen.getByPlaceholderText("e.g. Task disabled")).toHaveValue("");
        });

        it("shows empty set when filters match nothing", async () => {
            const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Reason" }));
            await fillInput(screen.getByPlaceholderText("e.g. Task disabled"), "no such reason");

            expect(await screen.findByText("No decisions match the current filters")).toBeInTheDocument();
        });

        it("clear removes all filters", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Event" }));
            await fireClick(screen.getByRole("checkbox", { name: "Failed" }));
            await fireClick(screen.getByRole("button", { name: /Back/ }));
            await fireClick(screen.getByRole("button", { name: "Clear" }));

            expect(screen.queryByRole("button", { name: "Event: Failed" })).not.toBeInTheDocument();
            expect(screen.getByText(infoEntry.Reason)).toBeInTheDocument();
        });

        it("opens the chip category menu, also when the filter menu is already open", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            await fireClick(screen.getByRole("button", { name: "Event" }));
            await fireClick(screen.getByRole("checkbox", { name: "Failed" }));
            await fireClick(screen.getByRole("button", { name: /Filter/ }));
            expect(screen.queryByRole("checkbox", { name: "Failed" })).not.toBeInTheDocument();

            await fireClick(screen.getByRole("button", { name: "Event: Failed" }));
            expect(screen.getByRole("checkbox", { name: "Failed" })).toBeInTheDocument();

            await fireClick(screen.getByRole("button", { name: "Back" }));
            await fireClick(screen.getByRole("button", { name: "Reason" }));
            expect(screen.queryByRole("checkbox", { name: "Failed" })).not.toBeInTheDocument();

            await fireClick(screen.getByRole("button", { name: "Event: Failed" }));
            expect(screen.getByRole("checkbox", { name: "Failed" })).toBeInTheDocument();
        });
    });
});
