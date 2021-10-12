import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteProvider } from "./common";
import { RqlParser } from "../RqlParser";

export class AutocompleteOrderBy extends BaseAutocompleteProvider implements AutocompleteProvider {

    async collectAsync(scanner: Scanner, candidates: CandidatesCollection): Promise<autoCompleteWordList[]> {
        if (candidates.rules.has(RqlParser.RULE_orderByOrder)) {
            return [{
                value: "asc ",
                caption: "asc",
                meta: AUTOCOMPLETE_META.keyword,
                score: AUTOCOMPLETE_SCORING.keyword
            }, {
                value: "desc ",
                caption: "desc",
                meta: AUTOCOMPLETE_META.keyword,
                score: AUTOCOMPLETE_SCORING.keyword
            }]
        }
        
        if (candidates.rules.has(RqlParser.RULE_orderBySortingAs)) {
            const sortings = ["string", "alphanumeric", "long", "double"];
            
            return sortings.map(s => ({
                caption: s, 
                value: s + " ",
                meta: AUTOCOMPLETE_META.keyword,
                score: AUTOCOMPLETE_SCORING.keyword
            }));
        }
        
        return [];
    }
}
