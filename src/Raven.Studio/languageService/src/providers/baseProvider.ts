import { CollectionByIndexContext, CollectionByNameContext, ProgContext } from "../generated/BaseRqlParser";
import { CandidateRule, CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";

export type QueryType = "index" | "collection" | "unknown";
export type QuoteType = "None" | "Single" | "Double";


const ident = x => x;

export function filterTokens<T>(text: string, candidates: T[], extractor: (val: T) => string = ident) {
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

export abstract class BaseAutocompleteProvider {
    
    protected readonly metadataProvider: queryCompleterProviders;

    constructor(metadataProvider: queryCompleterProviders) {
        this.metadataProvider = metadataProvider;
    }

    async getPossibleFields(parseTree: ProgContext, prefix: string): Promise<string[]> {
        const [queryType, source] = BaseAutocompleteProvider.detectQueryType(parseTree);

        return new Promise<string[]>(resolve => {
            switch (queryType) {
                case "unknown":
                    resolve([]);
                    break;
                case "index":
                    this.metadataProvider.indexFields(source, resolve);
                    break;
                case "collection":
                    this.metadataProvider.collectionFields(source, prefix, fields => resolve(Object.keys(fields)));
                    break;
            }
        });
    }
    
    static detectQuoteType(input: string): QuoteType {
        if (!input) {
            return "None";
        }

        switch (input[0]) {
            case "'":
                return "Single";
            case '"':
                return "Double";
            default:
                return "None";
        }
    }
    
    static quote(input: string, quoteType: QuoteType): string {
        switch (quoteType) {
            case "None":
                return input;
            case "Double":
                return `"${input.replace(/"/g, '\"')}"`;
            case "Single":
                return `'${input.replace(/'/g, "\'")}'`;
        }
    }
    
    static unquote(input: string): string {
        if (!input) {
            return input;
        }
        
        if (input.startsWith("'") && input.endsWith("'")) {
            const unquoted = input.substring(1, input.length - 1);
            return unquoted.replace(/\\'/g, "'");
        }
        
        if (input.startsWith('"') && input.endsWith('"')) {
            const unquoted = input.substring(1, input.length - 1);
            return unquoted.replace(/\\"/g, '"');
        }
        
        return input;
    }
    
    static detectQueryType(parseTree: ProgContext): [QueryType, string] {
        const from = parseTree.fromStatement();
        if (!from) {
            return ["unknown", undefined];
        }
        
        if (from instanceof CollectionByNameContext) {
            return ["collection", this.unquote(from.collectionName().text)];
        }

        if (from instanceof CollectionByIndexContext) {
            return ["index", this.unquote(from.indexName().text)];
        }
        
        return ["unknown", undefined];
    }
    
    static findLongestRuleStack(candidates: CandidatesCollection): number[] {
        const stacks = Array.from(candidates.rules.values()).map(x => x.ruleList);
        stacks.sort((a, b) => a.length - b.length);
        return stacks[0] || [];
    }
    
    static findFirstRule(candidates: CandidatesCollection, rules: number[]): [number, CandidateRule] {
        for (const rule of rules) {
            if (candidates.rules.has(rule)) {
                return [rule, candidates.rules.get(rule)];
            }
        }
        
        return null;
    }
}
