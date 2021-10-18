import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteProvider } from "./common";
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
            
            const mappedCollections = collections.map(c => ({
                meta: AUTOCOMPLETE_META.collection,
                score: AUTOCOMPLETE_SCORING.collection,
                caption: c,
                value: AutocompleteFrom.quote(c, quoteType === "Single" ? "Single" : "Double") + " "
            }));
            
            const indexSuggestion: autoCompleteWordList = {
                value: "index ",
                caption: "index",
                meta: AUTOCOMPLETE_META.keyword,
                score: AUTOCOMPLETE_SCORING.keyword
            };
            
            return [...mappedCollections, indexSuggestion];
        }
        
        if (candidates.rules.has(RqlParser.RULE_indexName)) {
            const indexes = await this.fetchIndexNames();
            return indexes.map(index => ({
                meta: AUTOCOMPLETE_META.index,
                score: AUTOCOMPLETE_SCORING.index,
                caption: index,
                value: AutocompleteFrom.quote(index, quoteType === "Single" ? "Single" : "Double") + " "
            }));
        }
        
        return [];
    }
}
