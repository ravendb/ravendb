import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { ProgContext, RqlParser } from "../generated/RqlParser";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import { META_FIELD, META_FUNCTION, SCORING_FIELD, SCORING_FUNCTION } from "./scoring";
import { AutocompleteProvider } from "./common";

export class AutocompleteGroupBy extends BaseAutocompleteProvider implements AutocompleteProvider {

    async getPossibleFields(parser: RqlParser): Promise<string[]> {
        const [queryType, source] = AutocompleteGroupBy.detectQueryType(parser);
        
        return new Promise<string[]>(resolve => {
            switch (queryType) {
                case "unknown":
                    resolve([]);
                    break;
                case "index":
                    this.metadataProvider.indexFields(source, resolve);
                    break;
                case "collection":
                    //TODO: pass correct prefix!
                    //TODO: extract and pass values as well!
                    this.metadataProvider.collectionFields(source, undefined, fields => resolve(Object.keys(fields)));
                    break;
            }
        });
    }
    
    async collectAsync(scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenPart: string): Promise<autoCompleteWordList[]> {
        const rule = AutocompleteGroupBy.findFirstRule(candidates, [RqlParser.RULE_function, RqlParser.RULE_variable, RqlParser.RULE_identifiersAllNames]);
               
        if (rule) {
            const matchedRule = rule[1];
            const inGroupBy = matchedRule.ruleList.length >= 2 && matchedRule.ruleList[0] === RqlParser.RULE_prog && matchedRule.ruleList[1] === RqlParser.RULE_groupByStatement;

            if (!inGroupBy) {
                return [];
            }
            
            const arrayFunction: autoCompleteWordList = {
                score: SCORING_FUNCTION,
                caption: "array()",
                value: "array(",
                meta: META_FUNCTION
            };
            
            const fields = await this.getPossibleFields(parser);
            
            const mappedFields = fields.map(f => ({
                meta: META_FIELD,
                score: SCORING_FIELD,
                value: f,
                caption: f
            }));
            
            return [arrayFunction, ...mappedFields];
        }
        
        return [];
    }
}
