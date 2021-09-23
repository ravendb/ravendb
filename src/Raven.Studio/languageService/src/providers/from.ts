import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { ProgContext, RqlParser } from "../generated/RqlParser";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import { META_COLLECTION, META_INDEX, SCORING_COLLECTION, SCORING_INDEX } from "./scoring";

const collections = ["Orders", "Products", "Employees"];
const indexes = ['Orders/ByCompany', 'Product/Rating'];

export class AutocompleteFrom extends BaseAutocompleteProvider {
    
    async collectAsync(scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenPart: string): Promise<autoCompleteWordList[]> {
        const quoteType = AutocompleteFrom.detectQuoteType(writtenPart);
        
        if (candidates.rules.has(RqlParser.RULE_collectionName)) {
            return collections.map(c => ({
                meta: META_COLLECTION,
                score: SCORING_COLLECTION,
                caption: c,
                value: AutocompleteFrom.quote(c, quoteType === "Single" ? "Single" : "Double") + " "
            }));
        }
        
        if (candidates.rules.has(RqlParser.RULE_indexName)) {
            return indexes.map(index => ({
                meta: META_INDEX,
                score: SCORING_INDEX,
                caption: index,
                value: AutocompleteFrom.quote(index, quoteType === "Single" ? "Single" : "Double") + " "
            }));
        }
        
        return [];
       
    }
}
