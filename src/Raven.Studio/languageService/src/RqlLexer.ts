import { BaseRqlLexer } from "./generated/BaseRqlLexer";
import { Vocabulary } from "antlr4ts/Vocabulary";
import { RavenVocabularyImpl } from "./RavenVocabularyImpl";

export class RqlLexer extends BaseRqlLexer {
    private readonly rqlVocabulary: Vocabulary = new RavenVocabularyImpl(BaseRqlLexer.VOCABULARY);
    
    get vocabulary(): Vocabulary {
        return this.rqlVocabulary;
    }
}
