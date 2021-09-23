import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { ProgContext, RqlParser } from "../generated/RqlParser";
import { Scanner } from "../scanner";


export interface AutocompleteProvider {
    collect?: (scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenText: string) => autoCompleteWordList[]; 
    collectAsync?: (scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenText: string) => Promise<autoCompleteWordList[]>;    
}
