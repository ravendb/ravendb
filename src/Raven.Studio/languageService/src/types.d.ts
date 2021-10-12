import { ParseTree } from "antlr4ts/tree/ParseTree";
import { CommonTokenStream } from "antlr4ts";
import { ProgContext } from "./generated/BaseRqlParser";
import { RqlParser } from "./RqlParser";

export type CaretPosition = { line: number, column: number };
export type TokenPosition = { index: number, context: ParseTree, text: string };

export interface ParsedRql {
    parser: RqlParser;
    parseTree: ProgContext;
    tokenStream: CommonTokenStream;
}
