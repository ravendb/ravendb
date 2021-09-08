import { TokenPosition } from "../types";
import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { RqlParser } from "../generated/RqlParser";
import { TerminalNode } from "antlr4ts/tree/TerminalNode";
import { BaseAutocompleteProvider } from "./baseProvider";

function filterTokens(text: string, candidates: string[]) {
    if (text.trim().length == 0) {
        return candidates;
    } else {
        return candidates.filter(c => c.toLowerCase().startsWith(text.toLowerCase()));
    }
}

export class AutocompleteKeywords extends BaseAutocompleteProvider {
    
    constructor(private ignoredTokens: number[]) {
        super();
    }
    
    collect(position: TokenPosition, candidates: CandidatesCollection, parser: RqlParser): autoCompleteWordList[] {
        const completions: autoCompleteWordList[] = [];

        const tokens: string[] = [];
        candidates.tokens.forEach((_, k) => {
            const symbolicName = parser.vocabulary.getSymbolicName(k);
            if (symbolicName) {
                tokens.push(symbolicName.toLowerCase());
            }
        });

        const isIgnoredToken =
            position.context instanceof TerminalNode &&
            this.ignoredTokens.indexOf(position.context.symbol.type) >= 0;

        const textToMatch = isIgnoredToken ? '' : position.text;

        completions.push(...filterTokens(textToMatch, tokens).map(x => ({
            caption: x,
            value: x,
            score: 100,
            meta: "keyword"
        })));

        return completions;
    }
    
}
