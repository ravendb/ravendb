import { handleAutoComplete } from "../src/autocomplete";
import { CaretPosition } from "../src/types";

import 'jest-extended';


const caret = "|";

export async function autocomplete(input: string): Promise<autoCompleteWordList[]> {
    const caretIdx = input.indexOf(caret);
    if (caretIdx === -1) {
        throw new Error("Unable to find caret (|) in input string: " + input);
    }
    
    const lines = input.split(/\r?\n/);
    const row = lines.findIndex(x => x.includes(caret));
    const column = lines[row].indexOf("|");

    const mappedLines = lines
        .map((line, idx) => idx === row ? line.replace("|", "") : line);
    
    const inputWithoutCaret = mappedLines.join("\r\n");
    const caretPosition: CaretPosition = {
        column,
        line: row + 1
    };

    return await handleAutoComplete(inputWithoutCaret, caretPosition);
}
