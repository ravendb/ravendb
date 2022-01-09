import { BaseAutocompleteProvider } from "./baseProvider";
import { AUTOCOMPLETE_META, AUTOCOMPLETE_SCORING, AutocompleteContext, AutocompleteProvider } from "./common";
import { RqlParser } from "../RqlParser";
import { QuoteUtils } from "../quoteUtils";

export class AutocompleteFrom extends BaseAutocompleteProvider implements AutocompleteProvider {
    
    async fetchCollectionNames(): Promise<string[]> {
        return new Promise<string[]>(resolve => this.metadataProvider.collections(resolve));
    }
    
    async fetchIndexNames(): Promise<string[]> {
        return new Promise<string[]>(resolve => this.metadataProvider.indexNames(resolve));
    }
    
    async collectAsync(ctx: AutocompleteContext): Promise<autoCompleteWordList[]> {
        const { writtenText, candidates } = ctx;
        const quoteType = AutocompleteFrom.detectQuoteType(writtenText);
        
        if (candidates.rules.has(RqlParser.RULE_collectionName)) {
            const collections = await this.fetchCollectionNames();
            
            const mappedCollections = collections.map(c => ({
                meta: AUTOCOMPLETE_META.collection,
                score: AUTOCOMPLETE_SCORING.collection,
                caption: c,
                value: QuoteUtils.quote(c, quoteType === "Single" ? "Single" : "Double") + " "
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
                value: QuoteUtils.quote(index, quoteType === "Single" ? "Single" : "Double") + " "
            }));
        }
        
        return [];
    }
}
