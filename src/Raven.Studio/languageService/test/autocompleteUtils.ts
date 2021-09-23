import { handleAutoComplete } from "../src/autocomplete";
import { CaretPosition } from "../src/types";

import 'jest-extended';


const caret = "|";

export function extractCaretPosition(input: string): { inputWithoutCaret: string, position: CaretPosition } {
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
    const position: CaretPosition = {
        column,
        line: row + 1
    };
    
    return {
        inputWithoutCaret, 
        position
    }
}

export async function autocomplete(input: string): Promise<autoCompleteWordList[]> {
    const { inputWithoutCaret, position } = extractCaretPosition(input);
    return await handleAutoComplete(inputWithoutCaret, position);
}
