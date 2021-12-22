import { CandidateRule, CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";

export type QuerySource = "index" | "collection" | "unknown";
export type QuoteType = "None" | "Single" | "Double";

const ident = (x: any) => x;

export function filterTokens<T>(text: string, candidates: T[], extractor: (val: T) => string = ident) {
    if (text.trim().length == 0 || candidates.length === 0) {
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

    async getPossibleFields(querySource: QuerySource, sourceName: string, prefix: string): Promise<string[]> {
        return new Promise<string[]>(resolve => {
            switch (querySource) {
                case "unknown":
                    resolve([]);
                    break;
                case "index":
                    this.metadataProvider.indexFields(sourceName, resolve);
                    break;
                case "collection":
                    this.metadataProvider.collectionFields(sourceName, prefix, fields => resolve(Object.keys(fields)));
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
