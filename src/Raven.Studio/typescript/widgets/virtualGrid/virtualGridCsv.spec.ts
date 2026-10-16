import textColumn from "widgets/virtualGrid/columns/textColumn";
import actionColumn from "widgets/virtualGrid/columns/actionColumn";
import { gridToCsv } from "./virtualGridCsv";

interface request {
    action: string;
    sizeInBytes: number;
    details: string;
}

const items: request[] = [
    { action: "Query (Orders/Totals)", sizeInBytes: 598405, details: "from Orders where Total > 100 & Freight < 5" },
    { action: "GetDocumentsAsync", sizeInBytes: 623543, details: "?startsWith=orders/" },
];

describe("virtualGridCsv", () => {
    describe("gridToCsv", () => {
        it("writes a header row and one row per item with the raw cell values", () => {
            const columns = [
                new textColumn<request>(null, (x) => x.action, "Action", "30%"),
                new textColumn<request>(null, "details", "Details", "70%"),
            ];

            expect(gridToCsv(columns, items)).toBe(
                "Action,Details\r\n" +
                    "Query (Orders/Totals),from Orders where Total > 100 & Freight < 5\r\n" +
                    "GetDocumentsAsync,?startsWith=orders/"
            );
        });

        it("uses the export value and header of a column that formats its cells", () => {
            const columns = [
                new textColumn<request>(null, (x) => `${(x.sizeInBytes / 1024).toFixed(2)} KB`, "Docs size", "20%", {
                    exportValue: (x) => x.sizeInBytes,
                    exportHeader: "Docs size (bytes)",
                }),
            ];

            expect(gridToCsv(columns, items)).toBe("Docs size (bytes)\r\n598405\r\n623543");
        });

        it("leaves out action columns", () => {
            const columns = [
                new actionColumn<request>(null, () => undefined, "Preview", "Preview", "70px"),
                new textColumn<request>(null, (x) => x.action, "Action", "100%"),
            ];

            expect(gridToCsv(columns, items)).toBe("Action\r\nQuery (Orders/Totals)\r\nGetDocumentsAsync");
        });
    });
});
