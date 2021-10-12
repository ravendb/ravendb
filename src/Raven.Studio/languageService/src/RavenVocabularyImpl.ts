import { Vocabulary } from "antlr4ts/Vocabulary";
import { RqlParser } from "./RqlParser";

export class RavenVocabularyImpl implements Vocabulary {
    
    private readonly parent: Vocabulary;
    
    public constructor(parent: Vocabulary) {
        this.parent = parent;
    }
    
    getDisplayName(tokenType: number): string {
        if (tokenType === RqlParser.GROUP_BY) {
            return "group by";
        }
        if (tokenType === RqlParser.ORDER_BY) {
            return "order by";
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
