import { TokenPosition } from "../types";
import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { RqlParser } from "../generated/RqlParser";


export interface AutocompleteProvider {
    collect?: (position: TokenPosition, candidates: CandidatesCollection, parser: RqlParser) => autoCompleteWordList[]; 
    collectAsync?: (position: TokenPosition, candidates: CandidatesCollection, parser: RqlParser) => Promise<autoCompleteWordList[]>;    
}
