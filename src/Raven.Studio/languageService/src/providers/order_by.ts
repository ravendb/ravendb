import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { ProgContext, RqlParser } from "../generated/RqlParser";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import {
    META_KEYWORD, SCORING_KEYWORD,
} from "./scoring";
import { AutocompleteProvider } from "./common";

export class AutocompleteOrderBy extends BaseAutocompleteProvider implements AutocompleteProvider {

    async collectAsync(scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenPart: string): Promise<autoCompleteWordList[]> {
        if (candidates.rules.has(RqlParser.RULE_orderByOrder)) {
            return [{
                value: "asc ",
                caption: "asc",
                meta: META_KEYWORD,
                score: SCORING_KEYWORD
            }, {
                value: "desc ",
                caption: "desc",
                meta: META_KEYWORD,
                score: SCORING_KEYWORD
            }]
        }
        
        if (candidates.rules.has(RqlParser.RULE_orderBySortingAs)) {
            const sortings = ["string", "alphanumeric", "long", "double"];
            
            return sortings.map(s => ({
                caption: s, 
                value: s + " ",
                meta: META_KEYWORD,
                score: SCORING_KEYWORD
            }));
        }
        
        return [];
    }
}
