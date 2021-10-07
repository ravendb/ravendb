import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { ProgContext, RqlParser } from "../generated/RqlParser";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import {
    META_ALL,
    META_COLLECTION,
    META_FUNCTION, META_KEYWORD,
    META_OPERATOR,
    SCORING_COLLECTION,
    SCORING_FUNCTION, SCORING_KEYWORD,
    SCORING_OPERATOR
} from "./scoring";
import { AutocompleteProvider } from "./common";

const ident = x => x;

function filterTokens<T>(text: string, candidates: T[], extractor: (val: T) => string = ident) {
    if (text.trim().length == 0) {
        return candidates;
    } else {
        return candidates.filter(c => {
            const startsWith = extractor(c).toLowerCase().startsWith(text.toLowerCase());
            const equals = extractor(c).toLowerCase() === text.toLowerCase();
            return startsWith && !equals;
        });
    }
}


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

const tokensRemap = new Map<number, string>();
tokensRemap.set(RqlParser.ALL_DOCS, "@all_docs");
tokensRemap.set(RqlParser.ORDER_BY, "order by");
tokensRemap.set(RqlParser.GROUP_BY, "group by");

const specialFunctions: Pick<autoCompleteWordList, "value" | "caption">[] = [
    {
        value: "fuzzy(",
        caption: "fuzzy(fieldName, factor)"
    },
    {
        value: "search(",
        caption: "search(field, terms, operator)"
    },
    {
        value: "facet(",
        caption: "facet()" //TODO: args
    },
    {
        value: "boost(",
        caption: "boost()" //TODO: args
    }, 
    {
        value: "startsWith(",
        caption: "startsWith()" //TODO: args
    },
    {
        value: "endsWith(",
        caption: "endsWith()" //TODO: args
    }, 
    {
        value: "moreLikeThis(",
        caption: "moreLikeThis()" //TODO args
    },
    {
        value: "intersect(",
        caption: "intersect()" //TODO: args
    },
    {
        value: "exact(",
        caption: "exact()" //TODO:
    }
]

const alreadyHandledTokenTypes: number[] = [
    RqlParser.MATH,
    RqlParser.EQUAL,
    RqlParser.METADATA,
    RqlParser.ALL_DOCS,
    ...rootKeywords
] 

export class AutocompleteKeywords extends BaseAutocompleteProvider implements AutocompleteProvider {
    
    constructor(metadataProvider: queryCompleterProviders, private ignoredTokens: number[]) {
        super(metadataProvider);
    }
    
    static handleFromAlias(candidates: CandidatesCollection, scanner: Scanner): autoCompleteWordList[] {
        const aliasRule = candidates.rules.get(RqlParser.RULE_fromAlias);
        scanner.push();
        
        try {
            scanner.seek(aliasRule.startTokenIndex);
            const withAlias = scanner.tokenType() === RqlParser.AS;
            return withAlias ? [] : [{
                value: "as ",
                caption: "as",
                score: SCORING_OPERATOR,
                meta: META_OPERATOR
            }];
        } finally {
            scanner.pop();
        }
        
    }
    
    static tryHandleFromWithExplicitAlias(candidates: CandidatesCollection, scanner: Scanner): boolean {
        if (!candidates.rules.has(RqlParser.RULE_fromAlias)) {
            return false;
        }
        
        if (scanner.lookBack() !== RqlParser.AS) {
            return false;
        }
        
        const allIdentifiers = candidates.rules.get(RqlParser.RULE_fromAlias);

        return allIdentifiers.ruleList.indexOf(RqlParser.RULE_fromAlias) !== -1;
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
            score: SCORING_FUNCTION,
            meta: META_FUNCTION
        }));
    } 
    
    static handleEqual(): autoCompleteWordList[] {
        return [
            {
                value: "==",
                caption: "==",
                score: SCORING_OPERATOR,
                meta: META_OPERATOR
            }
        ] 
    }
    
    static handleMath(): autoCompleteWordList[] {
        return [
            {
                value: "<",
                caption: "<",
                score: SCORING_OPERATOR,
                meta: META_OPERATOR
            },
            {
                value: "<=",
                caption: "<=",
                score: SCORING_OPERATOR,
                meta: META_OPERATOR
            },
            {
                value: ">",
                caption: ">",
                score: SCORING_OPERATOR,
                meta: META_OPERATOR
            },
            {
                value: ">=",
                caption: ">=",
                score: SCORING_OPERATOR,
                meta: META_OPERATOR
            }
        ]
    }
    
    static handleMetadata(): autoCompleteWordList[] {
        return [
            {
                value: "@metadata",
                caption: "@metadata",
                score: SCORING_FUNCTION,
                meta: META_FUNCTION
            }
        ]
    }

    static handleAllDocs(): autoCompleteWordList {
        return {
            value: "@all_docs ",
            caption: "@all_docs",
            meta: META_COLLECTION,
            score: SCORING_COLLECTION
        }
    }
    
    static handleRootKeywords(candidates: CandidatesCollection, parser: RqlParser, writtenText: string): autoCompleteWordList[] {
        const result: autoCompleteWordList[] = [];

        // we iterate here in order keywords appear in RQL
        for (const keyword of rootKeywords) {
            if (candidates.tokens.has(keyword)) {
                const displayName = parser.vocabulary.getSymbolicName(keyword).toLowerCase(); 
                result.push({
                    caption: displayName,
                    value: displayName + " ",
                    meta: META_KEYWORD,
                    score: SCORING_KEYWORD
                });
            }
        }
        
        const fromKeywordIndex = result.findIndex(x => x.caption === "from");
        if (fromKeywordIndex !== -1) {
            result.splice(fromKeywordIndex, 0, {
                value: "from index ",
                caption: "from index",
                meta: META_KEYWORD,
                score: SCORING_KEYWORD
            })
        }
        
        return filterTokens(writtenText, result, x => x.value);
    }
    
    collect(scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenText: string): autoCompleteWordList[] {
        const completions: autoCompleteWordList[] = [];
        
        if (candidates.rules.has(RqlParser.RULE_fromAlias)) {
            const fromAlias = AutocompleteKeywords.handleFromAlias(candidates, scanner);
            completions.push(...fromAlias);
            if (!fromAlias.length) {
                // we are just after 'as' inside 'from' - skip keywords
                return completions;
            }
        }
        
        if (candidates.rules.has(RqlParser.RULE_specialFunctionName)) {
            completions.push(...AutocompleteKeywords.handleSpecialFunctions(candidates, writtenText));
        }
        
        if (candidates.tokens.has(RqlParser.EQUAL)) {
            completions.push(...AutocompleteKeywords.handleEqual())
        }
        
        if (candidates.tokens.has(RqlParser.MATH)) {
            completions.push(...AutocompleteKeywords.handleMath());
        }
        
        if (candidates.tokens.has(RqlParser.METADATA)) {
            completions.push(...AutocompleteKeywords.handleMetadata());
        }

        if (candidates.tokens.has(RqlParser.ALL_DOCS)) {
            completions.push(AutocompleteKeywords.handleAllDocs());
        }
        
        completions.push(...AutocompleteKeywords.handleRootKeywords(candidates, parser, writtenText));
        
        const tokens: string[] = [];
        candidates.tokens.forEach((_, k) => {
            const hasRemap = tokensRemap.get(k);
            if (hasRemap) {
                tokens.push(tokensRemap.get(k));
            } else {
                const symbolicName = parser.vocabulary.getSymbolicName(k);
                if (symbolicName && alreadyHandledTokenTypes.indexOf(k) === -1) {
                    tokens.push(symbolicName.toLowerCase());
                }
            }
        });
        
        completions.push(...filterTokens(writtenText, tokens).map(x => ({
            caption: x,
            value: x + " ",
            score: 100,
            meta: META_ALL
        })));

        return completions;
    }
    
}
