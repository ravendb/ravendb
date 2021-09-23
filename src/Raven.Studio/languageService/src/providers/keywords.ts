import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { RqlParser } from "../generated/RqlParser";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import { META_ALL, META_FUNCTION, META_OPERATOR, SCORING_FUNCTION, SCORING_OPERATOR } from "./scoring";

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
export class AutocompleteKeywords extends BaseAutocompleteProvider {
    
    constructor(private ignoredTokens: number[]) {
        super();
    }
    
    tryHandleFromWithExplicitAlias(candidates: CandidatesCollection, scanner: Scanner): boolean {
        if (!candidates.rules.has(RqlParser.RULE_identifiersAllNames)) {
            return false;
        }
        
        if (scanner.lookBack() !== RqlParser.AS) {
            return false;
        }
        
        const allIdentifiers = candidates.rules.get(RqlParser.RULE_identifiersAllNames);

        return allIdentifiers.ruleList.indexOf(RqlParser.RULE_fromAlias) !== -1;
    }
    
    handleSpecialFunctions(candidates: CandidatesCollection, writtenText: string): autoCompleteWordList[] {
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
    
    handleEqual(): autoCompleteWordList[] {
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
    
    collect(scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, writtenText: string): autoCompleteWordList[] {
        const completions: autoCompleteWordList[] = [];
        
        if (this.tryHandleFromWithExplicitAlias(candidates, scanner)) {
            return completions;
        }
        
        if (candidates.rules.has(RqlParser.RULE_specialFunctionName)) {
            completions.push(...this.handleSpecialFunctions(candidates, writtenText));
        }
        
        if (candidates.tokens.has(RqlParser.EQUAL)) {
            completions.push(...this.handleEqual())
        }
        
        if (candidates.tokens.has(RqlParser.MATH)) {
            completions.push(...this.handleMath());
        }
        
        if (candidates.tokens.has(RqlParser.METADATA)) {
            completions.push(...this.handleMetadata());
        }
        
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
