import { ParseTree } from "antlr4ts/tree/ParseTree";
import { CollectionByIndexContext, CollectionByNameContext, ProgContext } from "../generated/BaseRqlParser";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";
import { CandidateRule, CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { RqlParser } from "../RqlParser";

export type QueryType = "index" | "collection" | "unknown";
export type QuoteType = "None" | "Single" | "Double";

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
    
    /**
     * Returns root context for given node, ex.: from, include, group by, etc
     */
    static findRootContext(tree: ParseTree) {
        if (tree instanceof TerminalNode && tree.parent instanceof ProgContext) {
            const progContext = tree.parent;
            const terminalNodeIdx = progContext.children.findIndex(x => x === tree);
            if (terminalNodeIdx > 0) {
                return progContext.children[terminalNodeIdx - 1];
            } else {
                return undefined;
            }
        } 
        
        while (tree && !(tree.parent instanceof ProgContext)) {
            tree = tree.parent;
        }
        
        return tree;
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
    
    static findFirstRule(candidates: CandidatesCollection, rules: number[]): [number, CandidateRule] {
        for (const rule of rules) {
            if (candidates.rules.has(rule)) {
                return [rule, candidates.rules.get(rule)];
            }
        }
        
        return null;
    }
}
