import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteProvider } from "./common";
import { RqlParser } from "../RqlParser";
import { ProgContext } from "../generated/BaseRqlParser";

export class AutocompleteGroupBy extends BaseAutocompleteProvider implements AutocompleteProvider {
    
    canCompleteArray(stack: number[]) {
        return stack[stack.length - 1] === RqlParser.RULE_function;
    }
    
    async collectAsync(scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenPart: string): Promise<autoCompleteWordList[]> {
        const stack = AutocompleteGroupBy.findLongestRuleStack(candidates);
               
        if (stack.length >= 2 && stack[0] === RqlParser.RULE_prog && stack[1] === RqlParser.RULE_groupByStatement) {
            const arrayFunction: autoCompleteWordList = {
                score: AUTOCOMPLETE_SCORING.function,
                caption: "array()",
                value: "array(",
                meta: AUTOCOMPLETE_META.function
            };
            
            const result: autoCompleteWordList[] = [];

            if (this.canCompleteArray(stack)) {
                result.push(arrayFunction);
            }
            
            return result;
        }
        
        return [];
    }
}
