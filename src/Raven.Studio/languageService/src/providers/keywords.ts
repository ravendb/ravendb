import { CandidateRule, CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { RqlParser } from "../RqlParser";
import { BaseAutocompleteProvider, filterTokens } from "./baseProvider";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteContext, AutocompleteProvider } from "./common";

const rootKeywords: number[] = [
    RqlParser.FROM,
    RqlParser.GROUP_BY,
    RqlParser.WHERE,
    RqlParser.LOAD,
    RqlParser.FILTER,
    RqlParser.FILTER_LIMIT,
    RqlParser.ORDER_BY,
    RqlParser.SELECT,
    RqlParser.INCLUDE,
    RqlParser.LIMIT,
    RqlParser.OFFSET
];


const specialSelectFunctions: Pick<autoCompleteWordList, "value" | "caption">[] = [
    {
        value: "suggest(",
        caption: "suggest(field, terms)"
    }
];

const specialWhereFunctions: Pick<autoCompleteWordList, "value" | "caption">[] = [
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
    },
    {
        value: "regex(",
        caption: "regex(field, pattern)"
    }, 
    {
        value: "lucene(",
        caption: "lucene(field, whereClause, exact = false)"
    },
    {
        value: "exists(", 
        caption: "exists(field)"
    },
    {
        value: "proximity(",
        caption: "proximity(whereClause, proximity)"
    }
];

const alreadyHandledTokenTypes: number[] = [
    RqlParser.MATH,
    RqlParser.BETWEEN,
    RqlParser.IN, 
    RqlParser.ALL,
    RqlParser.EQUAL,
    RqlParser.METADATA,
    RqlParser.AS,
    RqlParser.ALL_DOCS,
    RqlParser.OR,
    RqlParser.AND,
    RqlParser.INDEX,
    RqlParser.DISTINCT,
    RqlParser.UPDATE,
    RqlParser.JS_SELECT,
    RqlParser.JS_FUNCTION_DECLARATION,
    ...rootKeywords
] 

export class AutocompleteKeywords extends BaseAutocompleteProvider implements AutocompleteProvider {
    
    constructor(metadataProvider: queryCompleterProviders, private ignoredTokens: number[]) {
        super(metadataProvider);
    }
    
    static handleSpecialFunctions(candidates: CandidatesCollection, writtenText: string): autoCompleteWordList[] {
        const specialFunctionRule = candidates.rules.get(RqlParser.RULE_specialFunctionName);
        
        
        const inWhereSpecialFunction = specialFunctionRule
            && specialFunctionRule.ruleList.length >= 1
            && specialFunctionRule.ruleList[1] === RqlParser.RULE_whereStatement;

        if (inWhereSpecialFunction) {
            return AutocompleteKeywords.handleSpecialWhereFunctions(candidates, specialFunctionRule, writtenText);
        }

        const inSelectSpecialFunction = specialFunctionRule
            && specialFunctionRule.ruleList.length >= 1
            && specialFunctionRule.ruleList[1] === RqlParser.RULE_selectStatement;

        if (inSelectSpecialFunction) {
            return AutocompleteKeywords.handleSpecialSelectFunction(candidates, specialFunctionRule, writtenText);
        }
        
        return [];
    } 
    
    static handleSpecialWhereFunctions(candidates: CandidatesCollection, rule: CandidateRule, writtenText: string) {
        // check if we are not inside another special function! - we can't nest them
        const inSpecialFunction = rule.ruleList.find(x => x === RqlParser.RULE_specialParam);

        if (inSpecialFunction) {
            return [];
        }

        return filterTokens(writtenText, specialWhereFunctions, x => x.value).map(x => ({
            ...x,
            score: AUTOCOMPLETE_SCORING.function,
            meta: AUTOCOMPLETE_META.function
        }));
    }

    static handleSpecialSelectFunction(candidates: CandidatesCollection, rule: CandidateRule, writtenText: string) {
        // check if we are not inside another special function! - we can't nest them
        const inSpecialFunction = rule.ruleList.find(x => x === RqlParser.RULE_specialParam);

        if (inSpecialFunction) {
            return [];
        }

        return filterTokens(writtenText, specialSelectFunctions, x => x.value).map(x => ({
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
            },
            {
                value: "!=",
                caption: "!=",
                score: AUTOCOMPLETE_SCORING.operator,
                meta: AUTOCOMPLETE_META.operator
            },
            {
                value: "<>",
                caption: "<>",
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

    static handleOrOperator(): autoCompleteWordList {
        return {
            value: "or ", 
            caption: "or",
            meta: AUTOCOMPLETE_META.operator,
            score: AUTOCOMPLETE_SCORING.operator
        }
    }

    static handleAndOperator(): autoCompleteWordList {
        return {
            value: "and ",
            caption: "and",
            meta: AUTOCOMPLETE_META.operator,
            score: AUTOCOMPLETE_SCORING.operator
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

    static handleInOperator(): autoCompleteWordList {
        return {
            value: "in ()", 
            caption: "in ()",
            meta: AUTOCOMPLETE_META.keyword,
            score: AUTOCOMPLETE_SCORING.keyword,
            snippet: `in (\${1}) `
        }
    }

    static handleBetweenOperator(): autoCompleteWordList {
        return {
            value: "between ",
            caption: "between",
            meta: AUTOCOMPLETE_META.keyword,
            score: AUTOCOMPLETE_SCORING.keyword,
            snippet: `between \${1} and \${2} `
        }
    }

    static handleAllOperator(): autoCompleteWordList {
        return {
            value: "all in ()", 
            caption: "all in (...)",
            meta: AUTOCOMPLETE_META.keyword,
            score: AUTOCOMPLETE_SCORING.keyword,
            snippet: `all in (\${1}) `
        }
    }

    static handleDistinct(): autoCompleteWordList {
        return {
            value: "distinct ",
            caption: "distinct",
            score: AUTOCOMPLETE_SCORING.keyword,
            meta: AUTOCOMPLETE_META.keyword
        }
    }
    
    static handleUpdate(): autoCompleteWordList {
        return {
            value: "update ",
            caption: "update",
            score: AUTOCOMPLETE_SCORING.keyword,
            meta: AUTOCOMPLETE_META.keyword,
            snippet: `update {
    \${1}        
}`
        }
    }
    
    static jsFunctionDeclaration(): autoCompleteWordList {
        return {
            caption: "declare function", 
            value: "declare ", 
            score: AUTOCOMPLETE_SCORING.function, 
            meta: AUTOCOMPLETE_META.function, 
            snippet: `declare function \${1:Name}() {
    \${0}
}
`
        }
    }
    
    static canUseRootKeyword(keyword: number, ctx: AutocompleteContext) {
        const querySource = ctx.queryMetaInfo.querySourceType;
        
        if (keyword === RqlParser.GROUP_BY && querySource === "index") {
            // Can't use 'group by' when querying on an Index. 'group by' can be used only when querying on collections.
            return false;
        }
        
        if (keyword === RqlParser.FILTER_LIMIT && !ctx.parseTree.filterStatement()) {
            // can't use 'filter limit' when 'filter' was NOT used. 
            return false;
        }
        
        return true;
    }
    
    static handleRootKeywords(ctx: AutocompleteContext): autoCompleteWordList[] {
        const { candidates, parser, parseTree, writtenText} = ctx;
        const result: autoCompleteWordList[] = [];
        
        if (candidates.rules.has(RqlParser.RULE_rootKeywords) && !candidates.rules.has(RqlParser.RULE_fromMode)) {
            // we use root keywords as escape hatch to allow field names like FROM, WHERE etc
            // if we have root keywords rule it means where are inside some block
            // so don't complete keywords
            return [];
        }

        // we iterate here in order keywords appear in RQL
        for (const keyword of rootKeywords) {
            if (candidates.tokens.has(keyword) && AutocompleteKeywords.canUseRootKeyword(keyword, ctx)) {
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
    
    collect(ctx: AutocompleteContext): autoCompleteWordList[] {
        const { candidates, writtenText, parser } = ctx;
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

        if (candidates.tokens.has(RqlParser.OR)) {
            completions.push(AutocompleteKeywords.handleOrOperator());
        }
        
        if (candidates.tokens.has(RqlParser.AND)) {
            completions.push(AutocompleteKeywords.handleAndOperator());
        }

        if (candidates.tokens.has(RqlParser.DISTINCT)) {
            completions.push(AutocompleteKeywords.handleDistinct());
        }

        if (candidates.tokens.has(RqlParser.UPDATE) && ctx.queryMetaInfo.queryType === "Update") {
            completions.push(AutocompleteKeywords.handleUpdate());
        }
        
        if (candidates.tokens.has(RqlParser.JS_FUNCTION_DECLARATION)) {
            completions.push(AutocompleteKeywords.jsFunctionDeclaration());
        }

        if (candidates.tokens.has(RqlParser.IN)) {
            completions.push(AutocompleteKeywords.handleInOperator());
        }
        
        if (candidates.tokens.has(RqlParser.ALL)) {
            completions.push(AutocompleteKeywords.handleAllOperator());
        }

        if (candidates.tokens.has(RqlParser.BETWEEN)) {
            completions.push(AutocompleteKeywords.handleBetweenOperator());
        }
        
        completions.push(...AutocompleteKeywords.handleRootKeywords(ctx));

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

        if (tokens.length) {
            console.log("REMAINING TOKENS = ", tokens);
        }
    }
    
}
