import { BaseAutocompleteProvider } from "./baseProvider";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteContext, AutocompleteProvider } from "./common";
import { RqlParser } from "../RqlParser";

export class AutocompleteOrderBy extends BaseAutocompleteProvider implements AutocompleteProvider {

    async collectAsync(ctx: AutocompleteContext): Promise<autoCompleteWordList[]> {
        const { candidates } = ctx;
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
