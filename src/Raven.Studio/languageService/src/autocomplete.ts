import { CaretPosition } from "./types";
import { parseRql } from "./parser";
import { RqlQueryVisitor } from "./rqlQueryVisitor";
import { ProgContext, RqlParser } from "./generated/RqlParser";
import { CodeCompletionCore, SymbolTable } from "antlr4-c3";
import { AutocompleteProvider } from "./providers/common";
import { AutocompleteFrom } from "./providers/from";
import { AutocompleteKeywords } from "./providers/keywords";
import { CommonTokenStream } from "antlr4ts";
import { Scanner } from "./scanner";
import { AutocompleteGroupBy } from "./providers/group_by";
import { AutocompleteOrderBy } from "./providers/order_by";

export const ignoredTokens: number[] = [
    RqlParser.EOF,
    RqlParser.CL_CUR,
    RqlParser.CL_PAR,
    RqlParser.CL_Q,
    RqlParser.COMMA,
    RqlParser.DOT,
    RqlParser.OP_CUR,
    RqlParser.OP_PAR,
    RqlParser.OP_Q,
    RqlParser.SLASH,
    RqlParser.COLON,
    RqlParser.SEMICOLON,
    RqlParser.BACKSLASH,
    RqlParser.PLUS,
    RqlParser.MINUS,
    RqlParser.AT,
    RqlParser.HASH,
    RqlParser.DOL,
    RqlParser.PERCENT,
    RqlParser.POWER,
    RqlParser.NUM,
    RqlParser.AMP,
    RqlParser.STAR,
    RqlParser.QUESTION_MARK,
    RqlParser.EXCLAMATION,
    RqlParser.FALSE,
    RqlParser.NOT,
    RqlParser.NULL,
    RqlParser.TRUE,
    RqlParser.JS_FUNCTION_DECLARATION
];

const noSeparatorRequiredFor = new Set<number>([
    RqlParser.OP_PAR,
    RqlParser.CL_PAR
]);

export function createScannerWithSeek(inputStream: CommonTokenStream, caretPosition: CaretPosition): { scanner: Scanner, caretIndex: number } {
    const scanner = new Scanner(inputStream);

    scanner.advanceToPosition(caretPosition.line, caretPosition.column);
    scanner.push();

    let caretIndex = scanner.tokenIndex;
     
    const inTheMiddleOfWhiteSpace = scanner.tokenType() === RqlParser.WS && caretPosition.column !== scanner.tokenStart();
    
    if (caretIndex > 0 && !inTheMiddleOfWhiteSpace && !noSeparatorRequiredFor.has(scanner.lookBack())) {
        --caretIndex;
    }
    
    return {
        scanner,
        caretIndex
    }
}

export function getWrittenPart(input: string, caretPosition: CaretPosition) {
    const lines = input.split(/\r?\n/);
    const row = lines[caretPosition.line - 1];
    const toCaretText = row.substring(0, caretPosition.column);

    const tokens = toCaretText.split(/[ ,\\.)(\[\]]/);
    return tokens[tokens.length - 1];
}

export class autoCompleteEngine {
    
    private readonly metadataProvider: queryCompleterProviders;
    private readonly providers: AutocompleteProvider[];
    
    private debug: boolean = false; //TODO:
    
    public constructor(metadataProvider: queryCompleterProviders) {
        this.metadataProvider = metadataProvider;
        this.providers = this.defaultProviders(metadataProvider);
    }
    
    async complete(input: string, caret: CaretPosition): Promise<autoCompleteWordList[]> {
        const writtenPart = getWrittenPart(input, caret);

        const { parseTree, parser } = parseRql(input);

        return await this.getSuggestionsForParseTree(
            parser, parseTree, () => new RqlQueryVisitor().visit(parseTree), caret, writtenPart);
    }

    async getSuggestionsForParseTree(
        parser: RqlParser,
        parseTree: ProgContext,
        symbolTableFn: () => SymbolTable,
        caretPosition: CaretPosition,
        writtenPart: string
    ): Promise<autoCompleteWordList[]> {
        const core = new CodeCompletionCore(parser);

        core.ignoredTokens = new Set(ignoredTokens);

        core.preferredRules = new Set([
            RqlParser.RULE_collectionName,
            RqlParser.RULE_fromAlias,
            RqlParser.RULE_indexName,
            RqlParser.RULE_specialFunctionName,
            RqlParser.RULE_variable,
            RqlParser.RULE_function,
            RqlParser.RULE_orderBySortingAs,
            RqlParser.RULE_orderByOrder
        ]);

        core.translateRulesTopDown = true;

        if (this.debug) {
            core.showRuleStack = true;
            core.showDebugOutput = true;
            core.showResult = true;
        }

        const { scanner, caretIndex} = createScannerWithSeek(parser.inputStream as CommonTokenStream, caretPosition);

        const candidates = core.collectCandidates(caretIndex);

        const completions: autoCompleteWordList[] = [];

        for (const provider of this.providers) {
            if (provider.collect) {
                completions.push(...provider.collect(scanner, candidates, parser, parseTree, writtenPart));
            }
            if (provider.collectAsync) {
                const providerCompletions = await provider.collectAsync(scanner, candidates, parser, parseTree, writtenPart);
                if (providerCompletions.length > 0) {
                    completions.push(...providerCompletions);
                }
            }
        }

        return completions;
    }

    defaultProviders(metadataProvider: queryCompleterProviders): AutocompleteProvider[] {
        return [
            new AutocompleteKeywords(metadataProvider, ignoredTokens),
            new AutocompleteFrom(metadataProvider),
            new AutocompleteGroupBy(metadataProvider),
            new AutocompleteOrderBy(metadataProvider)
        ];
    }
}
