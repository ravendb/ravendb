import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { Scanner } from "../scanner";
import { RqlParser } from "../RqlParser";
import { ProgContext } from "../generated/BaseRqlParser";


export interface AutocompleteProvider {
    collect?: (scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenText: string) => autoCompleteWordList[]; 
    collectAsync?: (scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenText: string) => Promise<autoCompleteWordList[]>;    
}
