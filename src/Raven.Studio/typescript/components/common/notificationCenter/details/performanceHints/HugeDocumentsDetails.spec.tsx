import { rtlRender } from "test/rtlTestUtils";
import { NotificationStubs } from "test/stubs/NotificationStubs";
import HugeDocumentsDetails from "./HugeDocumentsDetails";
import { mockDetailsActions, mockTableWidth } from "../notificationDetailsTestUtils";

describe("HugeDocumentsDetails", () => {
    mockTableWidth();

    it("shows the documents newest first with a readable size", async () => {
        const hint = NotificationStubs.performanceHint("HugeDocuments", {
            HugeDocuments: {
                "orders/1": { Id: "orders/1", Size: 5 * 1024 * 1024, Date: "2026-10-20T10:00:00.0000000Z" },
                "orders/2": { Id: "orders/2", Size: 2048, Date: "2026-10-21T10:00:00.0000000Z" },
            },
        });

        const { screen } = rtlRender(<HugeDocumentsDetails notification={hint} actions={mockDetailsActions()} />);

        expect(await screen.findByText("2 documents")).toBeInTheDocument();
        expect(screen.getByText("5 MB")).toBeInTheDocument();
        expect(screen.getByText("2 KB")).toBeInTheDocument();

        const documentIds = screen.getAllByText(/^orders\//).map((element) => element.textContent);
        expect(documentIds).toEqual(["orders/2", "orders/1"]);
    });
});
