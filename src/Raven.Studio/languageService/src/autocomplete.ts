import { CaretPosition } from "./types";
import { parseRql } from "./parser";
import { computeTokenPosition } from "./caretPosition";
import { getSuggestionsForParseTree } from "./temp";
import { RqlQueryVisitor } from "./rqlQueryVisitor";

export async function handleAutoComplete(input: string, caret: CaretPosition): Promise<autoCompleteWordList[]> {
    const { parseTree, tokenStream, parser } = parseRql(input);

    const position = computeTokenPosition(parseTree, tokenStream, caret);

    if (!position) {
        return [];
    }
    return getSuggestionsForParseTree(
        parser, parseTree, () => new RqlQueryVisitor().visit(parseTree), position);
}
