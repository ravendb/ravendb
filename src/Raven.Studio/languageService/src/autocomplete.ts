import { CaretPosition } from "./types";
import { parseRql } from "./parser";
import { RqlQueryMetaInfo, RqlQueryVisitor } from "./rqlQueryVisitor";
import { RqlParser } from "./RqlParser";
import { CodeCompletionCore } from "antlr4-c3";
import { AutocompleteContext, AutocompleteProvider } from "./providers/common";
import { AutocompleteFrom } from "./providers/from";
import { AutocompleteKeywords } from "./providers/keywords";
import { CommonTokenStream } from "antlr4ts";
import { Scanner } from "./scanner";
import { AutocompleteGroupBy } from "./providers/group_by";
import { AutocompleteOrderBy } from "./providers/order_by";
import { ProgContext } from "./generated/BaseRqlParser";
import { AutoCompleteFields } from "./providers/fields";

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
    RqlParser.TIMESERIES_FUNCTION_DECLARATION
];

const noSeparatorRequiredFor = new Set<number>([
    RqlParser.OP_PAR,
    RqlParser.CL_PAR,
    RqlParser.DOT,
    RqlParser.COMMA
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
    
    scanner.seek(caretIndex);
    
    return {
        scanner,
        caretIndex
    }
}

export function getWrittenPart(input: string, caretPosition: CaretPosition) {
    const lines = input.split(/\r?\n/);
    const row = lines[caretPosition.line - 1];
    const toCaretText = row.substring(0, caretPosition.column);

    const tokens = toCaretText.split(/[ \\.,)(]/);
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
    
    async complete(input: string, caret: CaretPosition, queryType: rqlQueryType = "Select"): Promise<autoCompleteWordList[]> {
        const writtenPart = getWrittenPart(input, caret);

        const { parseTree, parser } = parseRql(input);

        return await this.getSuggestionsForParseTree(
            parser, parseTree, () => new RqlQueryVisitor(queryType).visit(parseTree), caret, writtenPart);
    }

    async getSuggestionsForParseTree(
        parser: RqlParser,
        parseTree: ProgContext,
        queryMetaInfoProvider: () => RqlQueryMetaInfo,
        caretPosition: CaretPosition,
        writtenText: string
    ): Promise<autoCompleteWordList[]> {
        const core = new CodeCompletionCore(parser);

        core.ignoredTokens = new Set(ignoredTokens);

        core.preferredRules = new Set([
            RqlParser.RULE_fromMode,
            RqlParser.RULE_collectionName,
            RqlParser.RULE_aliasName,
            RqlParser.RULE_indexName,
            RqlParser.RULE_specialFunctionName,
            RqlParser.RULE_variable,
            RqlParser.RULE_rootKeywords,
            RqlParser.RULE_function,
            RqlParser.RULE_orderBySortingAs,
            RqlParser.RULE_orderByOrder,
            RqlParser.RULE_identifiersWithoutRootKeywords,
            RqlParser.RULE_identifiersAllNames
        ]);

        core.translateRulesTopDown = true;

        if (this.debug) {
            core.showRuleStack = true;
            core.showDebugOutput = true;
            core.showResult = true;
        }
        
        const queryMetaInfo = queryMetaInfoProvider();
        
        const { scanner, caretIndex } = createScannerWithSeek(parser.inputStream as CommonTokenStream, caretPosition);

        const candidates = core.collectCandidates(caretIndex);

        const completions: autoCompleteWordList[] = [];

        const ctx: AutocompleteContext = {
            scanner,
            candidates,
            parser,
            parseTree,
            writtenText,
            queryMetaInfo
        };
        
        for (const provider of this.providers) {
            if (provider.collect) {
                completions.push(...provider.collect(ctx));
            }
            if (provider.collectAsync) {
                const providerCompletions = await provider.collectAsync(ctx);
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
            new AutoCompleteFields(metadataProvider),
            new AutocompleteGroupBy(metadataProvider),
            new AutocompleteOrderBy(metadataProvider)
        ];
    }
}
