import { TokenPosition } from "../types";
import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { RqlParser } from "../generated/RqlParser";
import { BaseAutocompleteProvider } from "./baseProvider";

export class AutocompleteFrom extends BaseAutocompleteProvider {
    
    async collectAsync(position: TokenPosition, candidates: CandidatesCollection, parser: RqlParser): Promise<autoCompleteWordList[]> {
        if (candidates.rules.has(RqlParser.RULE_collectionName)) {
            return [
                {
                    value: "Orders",
                    caption: "Orders",
                    score: 1000,
                    meta: "collection"
                }
            ]
        }
        
        if (candidates.rules.has(RqlParser.RULE_indexName)) {
            const quoteMode = position.text[0];
            switch (quoteMode) {
                case "'":
                    return [
                        {
                            value: `'Orders/ByName'`,
                            caption: "Orders/ByName",
                            score: 1000,
                            meta: "index"
                        }
                    ];
                case '"':
                default:
                    return [
                        {
                            value: `"Orders/ByName"`,
                            caption: "Orders/ByName",
                            score: 1000,
                            meta: "index"
                        }
                    ]
            }
        }
        
        return [];
       
    }
}
