import { AutocompleteProvider } from "./common";
import { ParseTree } from "antlr4ts/tree/ParseTree";
import { ProgContext } from "../generated/RqlParser";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";

export type QuoteType = "None" | "Single" | "Double"; 

export class BaseAutocompleteProvider implements AutocompleteProvider {
    
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
}
