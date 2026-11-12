import { PreviewLine, getDocumentPreviewLines } from "./documentPreviewLines";

const toText = (lines: PreviewLine[]) =>
    lines.map(({ depth, tokens }) => "    ".repeat(depth) + tokens.map((token) => token.text).join(""));

const editorText = (document: object) => JSON.stringify(document, null, 4).split("\n");

const territory = (index: number) => ({ Code: `T${index}`, Name: `Town ${index}` });
const territories = (count: number) => Array.from({ length: count }, (_, index) => territory(index + 1));

const roomyLineLimit = 100;

describe("getDocumentPreviewLines", () => {
    it("puts every property on its own line with the value in JSON form", () => {
        const { lines, hiddenLineCount } = getDocumentPreviewLines(
            { Company: "companies/65-A", Freight: 8.53, IsPaid: true, ShippedAt: null },
            roomyLineLimit
        );

        expect(toText(lines)).toEqual([
            "{",
            '    "Company": "companies/65-A",',
            '    "Freight": 8.53,',
            '    "IsPaid": true,',
            '    "ShippedAt": null',
            "}",
        ]);
        expect(hiddenLineCount).toBe(0);
    });

    it("writes the document as the document editor does", () => {
        const document: Record<string, unknown> = {
            Name: "Western",
            Tags: ["sale", "vip"],
            Territories: territories(2),
            Lines: [],
            Extra: {},
            ShipTo: { City: "Albuquerque", Location: { Latitude: 35.11, Longitude: -106.67 } },
            "@metadata": { "@collection": "Regions", "@id": "regions/2-A" },
        };

        const { lines } = getDocumentPreviewLines(document, roomyLineLimit);

        expect(toText(lines)).toEqual(editorText(document));
    });

    it("leaves out properties without a value, as JSON does", () => {
        const { lines } = getDocumentPreviewLines(
            {
                Lines: [{ Product: "products/2-A", Discount: undefined }],
                "@metadata": { "@collection": "Orders", "@flags": undefined },
            },
            roomyLineLimit
        );

        expect(toText(lines)).toEqual([
            "{",
            '    "Lines": [',
            "        {",
            '            "Product": "products/2-A"',
            "        }",
            "    ],",
            '    "@metadata": {',
            '        "@collection": "Orders"',
            "    }",
            "}",
        ]);
    });

    it("shows every line of a document that just fits", () => {
        const document = { Name: "Western", Territories: territories(2), Region: "West", Size: 4, Rank: 2 };

        const { lines, hiddenLineCount } = getDocumentPreviewLines(document, 16);

        expect(toText(lines)).toEqual(editorText(document));
        expect(hiddenLineCount).toBe(0);
    });

    it("keeps a line to tell how many lines are left when the document runs past the limit", () => {
        const document = {
            Name: "Eastern",
            Territories: territories(19),
            "@metadata": { "@collection": "Regions", "@id": "regions/1-A" },
        };

        const { lines, hiddenLineCount } = getDocumentPreviewLines(document, 13);

        expect(toText(lines)).toEqual(editorText(document).slice(0, 12));
        expect(hiddenLineCount).toBe(editorText(document).length - 12);
    });

    it("counts the lines of a long array it leaves out", () => {
        const document = { Embedding: Array.from({ length: 2000 }, (_, index) => index) };

        const { lines, hiddenLineCount } = getDocumentPreviewLines(document, 13);

        expect(toText(lines)).toEqual([
            "{",
            '    "Embedding": [',
            ...Array.from({ length: 10 }, (_, i) => `        ${i},`),
        ]);
        expect(hiddenLineCount).toBe(1992);
    });

    it("marks the lines of the document's metadata", () => {
        const { lines } = getDocumentPreviewLines(
            {
                Name: "Beverages",
                Origin: { "@metadata": { "@id": "categories/9-A" } },
                "@metadata": { "@collection": "Categories", "@id": "categories/1-A" },
            },
            roomyLineLimit
        );

        expect(toText(lines.filter((line) => line.isMetadata))).toEqual([
            '    "@metadata": {',
            '        "@collection": "Categories",',
            '        "@id": "categories/1-A"',
            "    }",
        ]);
    });

    it("tells keys, strings, other values and punctuation apart", () => {
        const { lines } = getDocumentPreviewLines(
            { Name: "Chang", Lines: [{ Product: "products/2-A", Quantity: 24 }] },
            roomyLineLimit
        );

        expect(lines[1].tokens).toEqual([
            { kind: "key", text: '"Name"' },
            { kind: "punctuation", text: ": " },
            { kind: "string", text: '"Chang"' },
            { kind: "punctuation", text: "," },
        ]);
        expect(lines[5].tokens).toContainEqual({ kind: "literal", text: "24" });
    });

    it("cuts a long string once the line runs past what the card can show", () => {
        const [, photoLine] = toText(
            getDocumentPreviewLines({ Photo: "iVBORw0KGgo".repeat(10_000) }, roomyLineLimit).lines
        );

        expect(photoLine.startsWith('    "Photo": "iVBORw0KGgoiVBORw0KGgo')).toBe(true);
        expect(photoLine.endsWith("…")).toBe(true);
        expect(photoLine.length).toBeLessThan(300);
    });
});
