import { autoCompleteEngine } from "../src/autocomplete";
import { CaretPosition } from "../src/types";

import 'jest-extended';
import { FakeMetadataProvider } from "./autocomplete/FakeMetadataProvider";
import { ANTLRErrorListener } from "antlr4ts/ANTLRErrorListener";
import { Token } from "antlr4ts/Token";
import { parseRqlOptions } from "../src/parser";

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

type ErrorItem = Parameters<ANTLRErrorListener<Token>["syntaxError"]>;

export class ErrorCollector {
    
    private _lexerErrors: ErrorItem[] = [];
    private _parserErrors: ErrorItem[] = [];
    
    numberOfLexerErrors() {
        return this._lexerErrors.length;
    }
    
    numberOfParserErrors() {
        return this._parserErrors.length;
    }
    
    numberOfErrors() {
        return this.numberOfLexerErrors() + this.numberOfParserErrors();
    }
    
    listeners(): Required<Pick<parseRqlOptions, "onLexerError" | "onParserError">> {
        const self = this;
        return {
            onParserError: function () { self._parserErrors.push([...arguments] as any); },
            onLexerError: function () { self._lexerErrors.push([...arguments] as any); }
        }
    }
}

export async function autocomplete(input: string, metadataProvider: queryCompleterProviders = new FakeMetadataProvider()): Promise<autoCompleteWordList[]> {
    const { inputWithoutCaret, position } = extractCaretPosition(input);
    const engine = new autoCompleteEngine(metadataProvider);
    return await engine.complete(inputWithoutCaret, position);
}
