import moment from "moment";
import { act, within } from "@testing-library/react";
import { composeStories } from "@storybook/react-webpack5";
import { mockServices } from "test/mocks/services/MockServices";
import { RtlScreen, rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import * as stories from "./BackupDecisionLog.stories";

const { BackupDecisionLogStory, ServerBlocked } = composeStories(stories);

const details = ManageServerStubs.backupDecisionLog();
const { Entries: entries } = details;
const reasonOf = (category: string) => entries.find((x) => x.Category === category).Reason;
const failedReason = reasonOf("Failed");
const serverStartupEntry = entries.find((x) => x.Detail === "Startup");
const heldBackReason = entries.find((x) => x.Detail === "BackupDisabled").Reason;

const watchMock = () => mockServices.manageServerService.mock.watchBackupDecisionLog as jest.Mock;
const lastQuery = () => watchMock().mock.lastCall[0] as string;
const cardOf = (screen: RtlScreen, title: string) =>
    within(screen.getByText(title, { selector: ".card-body *" }).closest(".card-body") as HTMLElement);

async function openFilterCategory(fireClick: (element: Element) => Promise<void>, screen: RtlScreen, name: string) {
    await fireClick(screen.getByRole("button", { name: /Filter/ }));
    await fireClick(screen.getByRole("button", { name }));
}

describe("BackupDecisionLog", () => {
    it("renders the snapshot of the current node", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

        expect(lastQuery()).toBe("take=512");
        expect(screen.getByText("Node A")).toBeInTheDocument();
        expect(screen.getByText(failedReason)).toBeInTheDocument();
        expect(screen.getByText(`Showing ${entries.length} events.`)).toBeInTheDocument();
    });

    it("shows empty state when there are no decisions", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(
            <BackupDecisionLogStory
                details={(x) => {
                    x.Entries = [];
                    x.TotalResults = 0;
                }}
            />
        );

        expect(screen.getByText("No decisions yet")).toBeInTheDocument();
    });

    it("shows load error when the connection fails before the snapshot and reconnects", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory isLoadFailing />);

        expect(screen.getByText(/Unable to load the backup decision log/)).toBeInTheDocument();

        mockServices.manageServerService.withBackupDecisionLog();
        await fireClick(screen.getByRole("button", { name: /Refresh/ }));

        expect(await screen.findByText(failedReason)).toBeInTheDocument();
    });

    it("merges updates by id, adds new decisions and shows the running total", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);
        const onMessage = watchMock().mock.lastCall[1];
        const now = moment().toISOString();

        act(() =>
            onMessage({
                ...details,
                Type: "Update",
                Databases: null,
                Tasks: null,
                Entries: [
                    { ...entries[0], Count: 6, LastTime: now },
                    {
                        ...entries[0],
                        Id: 100,
                        Category: "Started",
                        Detail: null,
                        Reason: "Started now",
                        FirstTime: now,
                        LastTime: now,
                        Count: 1,
                    },
                ],
                TotalResults: entries.length + 1,
            })
        );

        expect(screen.getByText("x 6")).toBeInTheDocument();
        expect(screen.getAllByText("x 5")).toHaveLength(2);
        expect(screen.getByText("Started now")).toBeInTheDocument();
        expect(screen.getByText(`Showing ${entries.length + 1} events.`)).toBeInTheDocument();
    });

    it("renders repeats with range and count", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

        expect(screen.getAllByText("x 5")).toHaveLength(3);
        expect(screen.getAllByText(/\d [AP]M - \d/)).toHaveLength(3);
    });

    it("renders server-wide and shard markers", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

        expect(screen.getAllByText("Server-wide")).toHaveLength(2);
        expect(screen.getAllByText("$0").length).toBeGreaterThan(0);
    });

    it("shows the server summary in cards", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

        expect(screen.getByText("1/4")).toBeInTheDocument();
        expect(screen.getByText("Up to 4 can run at once")).toBeInTheDocument();
        expect(screen.getByText(/^Next: task 'Daily backup' - \d\d:\d\d$/)).toBeInTheDocument();
        expect(cardOf(screen, "Waiting for schedule").getByText("2")).toBeInTheDocument();
        expect(cardOf(screen, "Held back").getByText(heldBackReason)).toBeInTheDocument();
        expect(screen.getByText(/^Failed - since \d\d:\d\d$/)).toBeInTheDocument();
        expect(screen.getByText(/^Latest: task 'Weekly full backup', \d\d:\d\d$/)).toBeInTheDocument();
    });

    it("shows a server-wide block in the held back card", async () => {
        const { screen } = await rtlRender_WithWaitForLoad(<ServerBlocked />);

        expect(
            cardOf(screen, "Held back").getByText(
                "All backups: The server is low on memory, scheduled backups are held back"
            )
        ).toBeInTheDocument();
    });

    describe("filters", () => {
        it("sends the selected events to the server", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await openFilterCategory(fireClick, screen, "Event");
            expect(screen.queryByRole("checkbox", { name: "Info" })).not.toBeInTheDocument();
            await fireClick(screen.getByRole("checkbox", { name: "Failed" }));

            expect(screen.getByRole("button", { name: "Event: Failed" })).toBeInTheDocument();
            expect(lastQuery()).toBe("take=512&category=Failed");
        });

        it("sends the reason text to the server", async () => {
            const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await openFilterCategory(fireClick, screen, "Reason");
            await fillInput(screen.getByPlaceholderText("e.g. Task disabled"), "unreachable");

            expect(await screen.findByRole("button", { name: "Reason: unreachable" })).toBeInTheDocument();
            expect(lastQuery()).toBe("take=512&reason=unreachable");
        });

        it("offers databases and tasks from the snapshot and sends the selection", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await openFilterCategory(fireClick, screen, "Database");
            await fireClick(screen.getByRole("checkbox", { name: "Invoices" }));
            await fireClick(screen.getByRole("button", { name: "Back" }));
            await fireClick(screen.getByRole("button", { name: "Task" }));
            await fireClick(screen.getByRole("checkbox", { name: "Hourly incremental (Invoices)" }));

            expect(screen.getByRole("button", { name: "Database: Invoices" })).toBeInTheDocument();
            expect(screen.getByRole("button", { name: "Task: Hourly incremental (Invoices)" })).toBeInTheDocument();
            expect(lastQuery()).toBe("take=512&database=Invoices&taskId=3");
        });

        it("shows load error when the filtered connection fails and reconnects", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            mockServices.manageServerService.withFailingBackupDecisionLog();
            await openFilterCategory(fireClick, screen, "Database");
            await fireClick(screen.getByRole("checkbox", { name: "Invoices" }));

            expect(await screen.findByText(/Unable to load the backup decision log/)).toBeInTheDocument();
            expect(screen.queryByText(failedReason)).not.toBeInTheDocument();

            mockServices.manageServerService.withBackupDecisionLog();
            await fireClick(screen.getByRole("button", { name: /Refresh/ }));

            expect(await screen.findByText(`Showing ${entries.length} events.`)).toBeInTheDocument();
            expect(lastQuery()).toBe("take=512&database=Invoices");
        });

        it("includes the whole selected To day", async () => {
            const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);
            const newestDay = moment(serverStartupEntry.LastTime).format("DD/MM/YYYY");

            await openFilterCategory(fireClick, screen, "Date range");
            await fillInput(screen.getAllByRole("textbox")[1], newestDay + " 00:00");

            expect(await screen.findByRole("button", { name: new RegExp(`- ${newestDay} 23:59`) })).toBeInTheDocument();
            expect(lastQuery()).toContain(
                "to=" + encodeURIComponent(moment(serverStartupEntry.LastTime).endOf("day").toISOString())
            );
        });

        it("clearing the reason chip closes the menu and empties the reason input", async () => {
            const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await openFilterCategory(fireClick, screen, "Reason");
            await fillInput(screen.getByPlaceholderText("e.g. Task disabled"), "no such reason");
            await screen.findByRole("button", { name: "Reason: no such reason" });

            await fireClick(screen.getByRole("button", { name: "Clear Reason filter" }));
            expect(screen.queryByPlaceholderText("e.g. Task disabled")).not.toBeInTheDocument();

            await openFilterCategory(fireClick, screen, "Reason");
            expect(screen.getByPlaceholderText("e.g. Task disabled")).toHaveValue("");
        });

        it("shows empty set when filters match nothing", async () => {
            const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            mockServices.manageServerService.withBackupDecisionLog((x) => {
                x.Entries = [];
                x.TotalResults = 0;
            });
            await openFilterCategory(fireClick, screen, "Reason");
            await fillInput(screen.getByPlaceholderText("e.g. Task disabled"), "no such reason");

            expect(await screen.findByText("No decisions match the current filters")).toBeInTheDocument();
        });

        it("clear removes all filters", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await openFilterCategory(fireClick, screen, "Event");
            await fireClick(screen.getByRole("checkbox", { name: "Failed" }));
            await fireClick(screen.getByRole("button", { name: /Back/ }));
            await fireClick(screen.getByRole("button", { name: "Clear" }));

            expect(screen.queryByRole("button", { name: "Event: Failed" })).not.toBeInTheDocument();
            expect(lastQuery()).toBe("take=512");
        });

        it("opens the chip category menu, also when the filter menu is already open", async () => {
            const { screen, fireClick } = await rtlRender_WithWaitForLoad(<BackupDecisionLogStory />);

            await openFilterCategory(fireClick, screen, "Event");
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
