import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { Scanner } from "../scanner";
import { RqlParser } from "../RqlParser";
import { ProgContext } from "../generated/BaseRqlParser";
import { RqlQueryMetaInfo } from "../rqlQueryVisitor";

export const AUTOCOMPLETE_SCORING = {
    operator: 1007,
    function: 1004,
    functionVectorTextual: 1023,
    functionVectorTextualOverload: 1022,
    functionVector: 1021,
    keyword: 1004,
    field: 1020,
    index: 1010,
    collection: 1030
}

export const AUTOCOMPLETE_META = {
    operator: "operator",
    keyword: "keyword",
    function: "function",
    field: "field",
    index: "index",
    collection: "collection"
};

export interface AutocompleteContext { 
    scanner: Scanner;
    candidates: CandidatesCollection;
    parser: RqlParser;
    parseTree: ProgContext;
    writtenText: string;
    queryMetaInfo: RqlQueryMetaInfo;
}

export interface AutocompleteProvider {
    collect?: (ctx: AutocompleteContext) => autoCompleteWordList[]; 
    collectAsync?: (ctx: AutocompleteContext) => Promise<autoCompleteWordList[]>;    
}
