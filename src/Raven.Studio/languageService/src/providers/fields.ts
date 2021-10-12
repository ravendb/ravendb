import { BaseAutocompleteProvider } from "./baseProvider";
import { AutocompleteProvider } from "./common";
import { Scanner } from "../scanner";
import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { RqlParser } from "../RqlParser";
import { ProgContext } from "../generated/BaseRqlParser";
import { META_FIELD, SCORING_FIELD } from "./scoring";

export class AutoCompleteFields extends BaseAutocompleteProvider implements AutocompleteProvider {

    async collectAsync(scanner: Scanner,
                       candidates: CandidatesCollection,
                       parser: RqlParser,
                       parseTree: ProgContext,
                       writtenPart: string,
    ): Promise<autoCompleteWordList[]> {
        if (candidates.rules.has(RqlParser.RULE_variable)) {
            const fields = await this.getPossibleFields(parseTree, writtenPart);
            
            return fields.map(field => ({
                meta: META_FIELD,
                score: SCORING_FIELD,
                caption: field,
                value: field
            }));
        }
        
        return [];
    }
}
