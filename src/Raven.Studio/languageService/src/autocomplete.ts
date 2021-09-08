import { CaretPosition, TokenPosition } from "./types";
import { parseRql } from "./parser";
import { computeTokenPosition } from "./caretPosition";
import { RqlQueryVisitor } from "./rqlQueryVisitor";
import { RqlParser } from "./generated/RqlParser";
import { ParseTree } from "antlr4ts/tree/ParseTree";
import { CodeCompletionCore, SymbolTable } from "antlr4-c3";
import { AutocompleteProvider } from "./providers/common";
import { AutocompleteFrom } from "./providers/from";
import { AutocompleteKeywords } from "./providers/keywords";

const ignoredTokens: number[] = [
    RqlParser.OP_PAR,
    RqlParser.CL_PAR,
    RqlParser.OP_Q,
    RqlParser.CL_Q,
    RqlParser.WORD,
    RqlParser.STRING,
    RqlParser.COMMA,
    RqlParser.DOL,
    RqlParser.NUM,
    RqlParser.PLUS,
    RqlParser.MINUS
];

const providers: AutocompleteProvider[] = [
    new AutocompleteKeywords(ignoredTokens),
    new AutocompleteFrom()
];

export async function getSuggestionsForParseTree(
    parser: RqlParser, parseTree: ParseTree, symbolTableFn: () => SymbolTable, position: TokenPosition): Promise<autoCompleteWordList[]> {
    const core = new CodeCompletionCore(parser);

    core.ignoredTokens = new Set(ignoredTokens);

    core.preferredRules = new Set([
        RqlParser.RULE_indexName,
        RqlParser.RULE_collectionName,
        RqlParser.RULE_identifiersNames,
        RqlParser.RULE_specialFunctionName
    ]);

    const candidates = core.collectCandidates(position.index);

    const completions: autoCompleteWordList[] = [];

    for (const provider of providers) {
        if (provider.collect) {
            completions.push(...provider.collect(position, candidates, parser));
        }
        if (provider.collectAsync) {
            const providerCompletions = await provider.collectAsync(position, candidates, parser);
            if (providerCompletions.length > 0) {
                completions.push(...providerCompletions);
            }
        }
    }

    return completions;
}

function handleEmptyQuery(): autoCompleteWordList[] {
    return [
        {
            value: "from ",
            score: 1000,
            caption: "from",
            meta: "from collection"
        },
        {
            value: "from index ",
            score: 1000,
            caption: "from index",
            meta: "from index"
        }
    ]
}

export async function handleAutoComplete(input: string, caret: CaretPosition): Promise<autoCompleteWordList[]> {
    const { parseTree, tokenStream, parser } = parseRql(input);

    const position = computeTokenPosition(parseTree, tokenStream, caret);

    if (!position) {
        return handleEmptyQuery();
    }
    return getSuggestionsForParseTree(
        parser, parseTree, () => new RqlQueryVisitor().visit(parseTree), position);
}
