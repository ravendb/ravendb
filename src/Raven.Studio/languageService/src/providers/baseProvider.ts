import { AutocompleteProvider } from "./common";
import { ParseTree } from "antlr4ts/tree/ParseTree";
import { ProgContext } from "../generated/RqlParser";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";

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
}
