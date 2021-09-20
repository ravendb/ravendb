import { ParseTree } from "antlr4ts/tree/ParseTree";
import { ProgContext, RqlParser } from "./generated/RqlParser";
import { CommonTokenStream } from "antlr4ts";

export type CaretPosition = { line: number, column: number };
export type TokenPosition = { index: number, context: ParseTree, text: string };

export interface ParsedRql {
    parser: RqlParser;
    parseTree: ProgContext;
    tokenStream: CommonTokenStream;
    jsTokenStream: CommonTokenStream;
}
