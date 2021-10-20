import { BaseAutocompleteProvider } from "./baseProvider";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteContext, AutocompleteProvider } from "./common";
import { RqlParser } from "../RqlParser";

export class AutocompleteGroupBy extends BaseAutocompleteProvider implements AutocompleteProvider {
    
    canCompleteArray(stack: number[]) {
        return stack[stack.length - 1] === RqlParser.RULE_function;
    }
    
    async collectAsync(ctx: AutocompleteContext): Promise<autoCompleteWordList[]> {
        const { candidates } = ctx;
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
