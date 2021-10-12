import { BaseAutocompleteProvider, filterTokens } from "./baseProvider";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteProvider } from "./common";
import { Scanner } from "../scanner";
import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { RqlParser } from "../RqlParser";
import { ProgContext } from "../generated/BaseRqlParser";

export class AutoCompleteFields extends BaseAutocompleteProvider implements AutocompleteProvider {
    
    findFieldPrefix(writtenPart: string, scanner: Scanner): string {
        scanner.push();
        
        const parts: string[] = [];
        
        try {
            while (scanner.lookBack() === RqlParser.DOT) {
                if (!scanner.previous()) {
                    break;
                }
                // at dot
                // check for []
                if (scanner.lookBack() === RqlParser.CL_Q) {
                    scanner.previous();
                    if (scanner.lookBack() === RqlParser.OP_Q) {
                        scanner.previous();
                    } else {
                        // unexpected //TODO: handle this case!
                        break;
                    }
                }
                
                // at literal / string
                if (scanner.previous()) {
                    switch (scanner.tokenType()) {
                        case RqlParser.STRING:
                            parts.unshift(AutoCompleteFields.unquote(scanner.tokenText()));
                            break;
                        case RqlParser.WORD:
                            parts.unshift(scanner.tokenText());
                            break;
                        default:
                            break;
                    }
                } else {
                    break; 
                }
            }
        } finally {
            scanner.pop();
        }
        
        return parts.join(".");
    }
    
    async collectAsync(scanner: Scanner,
                       candidates: CandidatesCollection,
                       parser: RqlParser,
                       parseTree: ProgContext,
                       writtenPart: string,
    ): Promise<autoCompleteWordList[]> {
        if (candidates.rules.has(RqlParser.RULE_variable)) {
            const rule = candidates.rules.get(RqlParser.RULE_variable);
            if (rule.startTokenIndex < scanner.tokenIndex && scanner.lookBack() !== RqlParser.DOT) {
                return [];
            }
            
            const fields = await this.getPossibleFields(parseTree, this.findFieldPrefix(writtenPart, scanner));

            return filterTokens(writtenPart, fields).map(field => ({
                meta: AUTOCOMPLETE_META.field,
                score: AUTOCOMPLETE_SCORING.field,
                caption: field,
                value: field
            }));
        }
        
        return [];
    }
}
