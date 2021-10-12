import { Vocabulary } from "antlr4ts/Vocabulary";
import { RqlParser } from "./RqlParser";

const keywordsRemap = new Map<number, string>();
keywordsRemap.set(RqlParser.GROUP_BY, "group by");
keywordsRemap.set(RqlParser.ORDER_BY, "order by");

export class RavenVocabularyImpl implements Vocabulary {
    
    private readonly parent: Vocabulary;
    
    public constructor(parent: Vocabulary) {
        this.parent = parent;
    }
    
    getDisplayName(tokenType: number): string {
        if (keywordsRemap.has(tokenType)) {
            return keywordsRemap.get(tokenType);
        }
        
        return this.parent.getDisplayName(tokenType);
    }

    get maxTokenType() {
        return this.parent.maxTokenType;
    }

    getLiteralName(tokenType: number): string | undefined {
        return this.parent.getLiteralName(tokenType);
    }

    getSymbolicName(tokenType: number): string | undefined {
        return this.parent.getSymbolicName(tokenType);
    }
}
