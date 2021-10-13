import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { RqlParser } from "../RqlParser";
import { BaseAutocompleteProvider, filterTokens } from "./baseProvider";
import { Scanner } from "../scanner";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteProvider } from "./common";
import { ProgContext } from "../generated/BaseRqlParser";

const rootKeywords: number[] = [
    RqlParser.FROM,
    RqlParser.GROUP_BY,
    RqlParser.WHERE,
    RqlParser.LOAD,
    RqlParser.ORDER_BY,
    RqlParser.SELECT,
    RqlParser.INCLUDE,
    RqlParser.LIMIT
];

const specialFunctions: Pick<autoCompleteWordList, "value" | "caption">[] = [
    {
        value: "fuzzy(",
        caption: "fuzzy(field = value, factor)"
    },
    {
        value: "search(",
        caption: "search(field, terms, operator = Guess)"
    },
    {
        value: "facet(",
        caption: "facet(field or facets)"
    },
    {
        value: "boost(",
        caption: "boost(expr)" 
    }, 
    {
        value: "startsWith(",
        caption: "startsWith(field, prefix)" 
    },
    {
        value: "endsWith(",
        caption: "endsWith(field, postfix)"
    }, 
    {
        value: "moreLikeThis(",
        caption: "moreLikeThis(id() == value, options?)"
    },
    {
        value: "intersect(",
        caption: "intersect(expr1, expr2, ...)"
    },
    {
        value: "exact(",
        caption: "exact(expr)"
    }
]

const alreadyHandledTokenTypes: number[] = [
    RqlParser.MATH,
    RqlParser.EQUAL,
    RqlParser.METADATA,
    RqlParser.AS,
    RqlParser.ALL_DOCS,
    ...rootKeywords
] 

export class AutocompleteKeywords extends BaseAutocompleteProvider implements AutocompleteProvider {
    
    constructor(metadataProvider: queryCompleterProviders, private ignoredTokens: number[]) {
        super(metadataProvider);
    }
    
    static handleFromAlias(candidates: CandidatesCollection, scanner: Scanner): autoCompleteWordList[] {
        const aliasRule = candidates.rules.get(RqlParser.RULE_aliasWithOptionalAs);
        scanner.push();
        
        try {
            scanner.seek(aliasRule.startTokenIndex);
            const withAlias = scanner.tokenType() === RqlParser.AS;
            return withAlias ? [] : [{
                value: "as ",
                caption: "as",
                score: AUTOCOMPLETE_SCORING.operator,
                meta: AUTOCOMPLETE_META.operator
            }];
        } finally {
            scanner.pop();
        }
        
    }
    
    static handleSpecialFunctions(candidates: CandidatesCollection, writtenText: string): autoCompleteWordList[] {
        const specialFunctionRule = candidates.rules.get(RqlParser.RULE_specialFunctionName);
        const inWhereSpecialFunction = specialFunctionRule
            && specialFunctionRule.ruleList.length >= 1
            && specialFunctionRule.ruleList[1] === RqlParser.RULE_whereStatement;

        if (!inWhereSpecialFunction) {
            return [];
        }

        return filterTokens(writtenText, specialFunctions, x => x.value).map(x => ({
            ...x,
            score: AUTOCOMPLETE_SCORING.function,
            meta: AUTOCOMPLETE_META.function
        }));
    } 
    
    static handleEqual(writtenText: string): autoCompleteWordList[] {
        if (writtenText.endsWith(".")) {
            return [];
        }
        
        return [
            {
                value: "==",
                caption: "==",
                score: AUTOCOMPLETE_SCORING.operator,
                meta: AUTOCOMPLETE_META.operator
            }
        ] 
    }
    
    static handleMath(writtenText: string): autoCompleteWordList[] {
        if (writtenText.endsWith(".")) {
            return [];
        }
        
        return [
            {
                value: "<",
                caption: "<",
                score: AUTOCOMPLETE_SCORING.operator,
                meta: AUTOCOMPLETE_META.operator
            },
            {
                value: "<=",
                caption: "<=",
                score: AUTOCOMPLETE_SCORING.operator,
                meta: AUTOCOMPLETE_META.operator
            },
            {
                value: ">",
                caption: ">",
                score: AUTOCOMPLETE_SCORING.operator,
                meta: AUTOCOMPLETE_META.operator
            },
            {
                value: ">=",
                caption: ">=",
                score: AUTOCOMPLETE_SCORING.operator,
                meta: AUTOCOMPLETE_META.operator
            }
        ]
    }
    
    static handleMetadata(): autoCompleteWordList[] {
        return [
            {
                value: "@metadata",
                caption: "@metadata",
                score: AUTOCOMPLETE_SCORING.function,
                meta: AUTOCOMPLETE_META.function
            }
        ]
    }

    static handleAsOperator(): autoCompleteWordList {
        return {
            value: "as ",
            caption: "as",
            meta: AUTOCOMPLETE_META.keyword,
            score: AUTOCOMPLETE_SCORING.keyword
        }
    }
    
    static handleAllDocs(): autoCompleteWordList {
        return {
            value: "@all_docs ",
            caption: "@all_docs",
            meta: AUTOCOMPLETE_META.collection,
            score: AUTOCOMPLETE_SCORING.collection
        }
    }
    
    static handleRootKeywords(candidates: CandidatesCollection, parser: RqlParser, writtenText: string): autoCompleteWordList[] {
        const result: autoCompleteWordList[] = [];

        // we iterate here in order keywords appear in RQL
        for (const keyword of rootKeywords) {
            if (candidates.tokens.has(keyword)) {
                const displayName = parser.vocabulary.getDisplayName(keyword).toLowerCase(); 
                result.push({
                    caption: displayName,
                    value: displayName + " ",
                    meta: AUTOCOMPLETE_META.keyword,
                    score: AUTOCOMPLETE_SCORING.keyword
                });
            }
        }
        
        const fromKeywordIndex = result.findIndex(x => x.caption === "from");
        if (fromKeywordIndex !== -1) {
            result.splice(fromKeywordIndex, 0, {
                value: "from index ",
                caption: "from index",
                meta: AUTOCOMPLETE_META.keyword,
                score: AUTOCOMPLETE_SCORING.keyword
            })
        }
        
        return filterTokens(writtenText, result, x => x.value);
    }
    
    collect(scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenText: string): autoCompleteWordList[] {
        const completions: autoCompleteWordList[] = [];
                
        if (candidates.rules.has(RqlParser.RULE_specialFunctionName)) {
            completions.push(...AutocompleteKeywords.handleSpecialFunctions(candidates, writtenText));
        }
        
        if (candidates.tokens.has(RqlParser.EQUAL)) {
            completions.push(...AutocompleteKeywords.handleEqual(writtenText))
        }
        
        if (candidates.tokens.has(RqlParser.MATH)) {
            completions.push(...AutocompleteKeywords.handleMath(writtenText));
        }
        
        if (candidates.tokens.has(RqlParser.METADATA)) {
            completions.push(...AutocompleteKeywords.handleMetadata());
        }

        if (candidates.tokens.has(RqlParser.ALL_DOCS)) {
            completions.push(AutocompleteKeywords.handleAllDocs());
        }

        if (candidates.tokens.has(RqlParser.AS)) {
            completions.push(AutocompleteKeywords.handleAsOperator());
        }
        
        completions.push(...AutocompleteKeywords.handleRootKeywords(candidates, parser, writtenText));

        AutocompleteKeywords.debugRemainingTokens(candidates, parser, writtenText); //TODO: comment out!
       
        return completions;
    }
    
    static debugRemainingTokens(candidates: CandidatesCollection, parser: RqlParser, writtenText: string) {
        const tokens: string[] = [];
        candidates.tokens.forEach((_, k) => {
            const displayName = parser.vocabulary.getDisplayName(k);
            if (displayName && alreadyHandledTokenTypes.indexOf(k) === -1) {
                tokens.push(displayName.toLowerCase());
            }
        }); 

        console.log("REMAINING TOKENS = ", ...filterTokens(writtenText, tokens));
    }
    
}
