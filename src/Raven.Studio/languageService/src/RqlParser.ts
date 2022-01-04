import { BaseRqlParser } from "./generated/BaseRqlParser";
import { Vocabulary } from "antlr4ts/Vocabulary";
import { RavenVocabularyImpl } from "./RavenVocabularyImpl";

export class RqlParser extends BaseRqlParser {
    private readonly rqlVocabulary: Vocabulary = new RavenVocabularyImpl(BaseRqlParser.VOCABULARY);

    get vocabulary(): Vocabulary {
        return this.rqlVocabulary;
    }
}
