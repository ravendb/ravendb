import { parseRql } from "../../src/parser";
import { createScannerWithSeek, getWrittenPart } from "../../src/autocomplete";
import { extractCaretPosition } from "../autocompleteUtils";
import { CommonTokenStream } from "antlr4ts";
import { RqlParser } from "../../src/RqlParser";

function act(input: string) {
    const { inputWithoutCaret, position } = extractCaretPosition(input);
    
    const { parser } = parseRql(inputWithoutCaret);

    const writtenPart = getWrittenPart(inputWithoutCaret, position);

    return {
        ...createScannerWithSeek(parser.inputStream as CommonTokenStream, { column: position.column - writtenPart.length, line: position.line}),
        writtenPart
    }
}

describe("scanner seek", function () {
    it("can seek on empty - w/o whitespace after", () => {
        const { writtenPart, caretIndex, scanner } = act("|");
        
        expect(caretIndex)
            .toEqual(0);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.EOF);
    })

    it("can seek on empty - w/ whitespace before", () => {
        const { writtenPart, caretIndex, scanner } = act(" | ");

        expect(caretIndex)
            .toEqual(0);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    })

    it("can seek on empty - w/ whitespace after", () => {
        const { writtenPart, caretIndex, scanner } = act("| ");

        expect(caretIndex)
            .toEqual(0);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    })
    
    it("can seek before empty collection - w/o whitespace after", () => {
        const { writtenPart, caretIndex, scanner } = act("from |");
        
        expect(caretIndex)
            .toEqual(1);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    });

    it("can seek before empty collection - w/ whitespace after", () => {
        const { writtenPart, caretIndex, scanner } = act("from | ");

        expect(caretIndex)
            .toEqual(1);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    });

    it("just after token - w/o space after", () => {
        const { writtenPart, caretIndex, scanner } = act(" from|");

        expect(caretIndex)
            .toEqual(0);
        expect(writtenPart)
            .toEqual("from");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    });

    it("just after token - w space after", () => {
        const { writtenPart, caretIndex, scanner } = act(" from| ");

        expect(caretIndex)
            .toEqual(0);
        expect(writtenPart)
            .toEqual("from");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    });
    
    it("can seek on partial collection - at the end", () => {
        const { writtenPart, caretIndex, scanner } = act("from Ord| ");

        expect(caretIndex)
            .toEqual(1);
        expect(writtenPart)
            .toEqual("Ord");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    });

    it("can seek on partial collection - in the middle", () => {
        const { writtenPart, caretIndex, scanner } = act("from Ord|ers ");

        expect(caretIndex)
            .toEqual(1);
        expect(writtenPart)
            .toEqual("Ord");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    });

    it("can seek on partial collection", () => {
        const { writtenPart, caretIndex, scanner } = act("from 'Ord|");

        expect(caretIndex)
            .toEqual(1);
        expect(writtenPart)
            .toEqual("'Ord");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    });
    
    it("can seek on after where - space after", () => {
        const { writtenPart, caretIndex, scanner } = act('from "Orders" where |  ');

        expect(caretIndex)
            .toEqual(5);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    })

    it("can seek on after where - no space after", () => {
        const { writtenPart, caretIndex, scanner } = act('from "Orders" where |');

        expect(caretIndex)
            .toEqual(5);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    });
    
    it("can seek after closing bracket - w/o space after", () => {
        const { writtenPart, caretIndex, scanner } = act('from Orders where (Name == "Test")|');

        expect(caretIndex)
            .toEqual(13);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.EOF);
    });

    it("can seek after closing bracket - w/ space after", () => {
        const { writtenPart, caretIndex, scanner } = act('from Orders where (Name == "Test")| ');

        expect(caretIndex)
            .toEqual(13);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    });
    
    it("can seek on nested path", () => {
        const { writtenPart, caretIndex, scanner } = act('from Orders where Lines.| ');

        expect(caretIndex)
            .toEqual(8);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
    })
    
    it("can ignore whitespace", () => {
        const query1 = act("from Orders group by |");
        const query2 = act("from Orders group by Co|");
        
        expect(query1.caretIndex)
            .toEqual(5);
        expect(query2.caretIndex)
            .toEqual(5);
    })
    
    it("can seek after declare function ", () => {
        const { writtenPart, caretIndex, scanner } = act("declare function x() {\n" +
            "    \n" +
            "}\n" +
            "\n" +
            "\n" +
            "|\n" +
            "from \n ");

        expect(caretIndex)
            .toEqual(16);
        expect(writtenPart)
            .toEqual("");
        expect(scanner.tokenType())
            .toEqual(RqlParser.WS);
        
        expect(scanner.lookAhead())
            .toEqual(RqlParser.FROM);
    });
});
