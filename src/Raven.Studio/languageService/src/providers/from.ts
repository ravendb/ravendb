import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import { META_COLLECTION, META_INDEX, SCORING_COLLECTION, SCORING_INDEX } from "./scoring";
import { AutocompleteProvider } from "./common";
import { RqlParser } from "../RqlParser";
import { ProgContext } from "../generated/BaseRqlParser";

export class AutocompleteFrom extends BaseAutocompleteProvider implements AutocompleteProvider {
    
    async fetchCollectionNames(): Promise<string[]> {
        return new Promise<string[]>(resolve => this.metadataProvider.collections(resolve));
    }
    
    async fetchIndexNames(): Promise<string[]> {
        return new Promise<string[]>(resolve => this.metadataProvider.indexNames(resolve));
    }
    
    async collectAsync(scanner: Scanner, 
                       candidates: CandidatesCollection, 
                       parser: RqlParser, 
                       parseTree: ProgContext, 
                       writtenPart: string, 
                       ): Promise<autoCompleteWordList[]> {
        const quoteType = AutocompleteFrom.detectQuoteType(writtenPart);
        
        if (candidates.rules.has(RqlParser.RULE_collectionName)) {
            const collections = await this.fetchCollectionNames();
            
            return collections.map(c => ({
                meta: META_COLLECTION,
                score: SCORING_COLLECTION,
                caption: c,
                value: AutocompleteFrom.quote(c, quoteType === "Single" ? "Single" : "Double") + " "
            }));
        }
        
        if (candidates.rules.has(RqlParser.RULE_indexName)) {
            const indexes = await this.fetchIndexNames();
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
