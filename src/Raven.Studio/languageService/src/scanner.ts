import { CommonTokenStream } from "antlr4ts";
import { Token } from "antlr4ts/Token";
import { Interval } from "antlr4ts/misc";

export class Scanner {
    private _index = 0;
    private readonly _tokens: Token[];
    private _tokenStack: number[] = [];
    
    constructor(input: CommonTokenStream) {
        input.fill();
        this._tokens = input.getTokens();
    }
    
    public next(skipHidden: boolean = true): boolean {
        while (this._index < this._tokens.length - 1) {
            ++this._index;
            if (this._tokens[this._index].channel == Token.DEFAULT_CHANNEL || !skipHidden) {
                return true;
            }
        }
        
        return false;
    }
    
    public previous(skipHidden = true): boolean {
        while (this._index > 0) {
            --this._index;
            
            if (this._tokens[this._index].channel === Token.DEFAULT_CHANNEL || !skipHidden) {
                return true;
            }
        }
        return false;
    }

    public advanceToPosition(line: number, offset: number): boolean {
        if (!this._tokens.length) {
            return false;
        }
        
        let i = 0;
        for (; i < this._tokens.length; i++) {
            const run = this._tokens[i];
            const tokenLine = run.line;
            if (tokenLine >= line) {
                const tokenOffset = run.charPositionInLine;
                const tokenLength = run.stopIndex - run.startIndex + 1;
                if (tokenLine === line && tokenOffset <= offset && offset < tokenOffset + tokenLength) {
                    this._index = i;
                    break;
                }
                
                if (tokenLine > line || tokenOffset > offset) {
                    // We reached a token after the current offset. Take the previous one as result then.
                    if (i === 0) {
                        return false;
                    }
                    
                    this._index = i - 1;
                    break;
                }
            }
        }

        if (i === this._tokens.length) {
            this._index = i - 1; // Nothing found, take the last token instead.
        }
        
        return true;
    }
    
    
    public advanceToType(type: number): boolean {
        for (let i = this._index; i < this._tokens.length; ++i) {
            if (this._tokens[i].type === type) {
                this._index = i;
                return true;
            }
        }
        
        return false;
    }
    
    public skipTokenSequence(sequence: number[]): boolean {
        if (this._index >= this._tokens.length) {
            return false;
        }

        for (let token of sequence) {
            if (this._tokens[this._index].type !== token) {
                return false;
            }
            
            while (++this._index < this._tokens.length && this._tokens[this._index].channel !== Token.DEFAULT_CHANNEL) {
                
            }

            if (this._index === this._tokens.length) {
                return false;
            }
        }
        
        return true;
    }

    public lookAhead(skipHidden: boolean = true) {
        let index = this._index;
        while (index < this._tokens.length - 1) {
            ++index;
            if (this._tokens[index].channel === Token.DEFAULT_CHANNEL || !skipHidden) {
                return this._tokens[index].type;
            }
        }
        
        return Token.INVALID_TYPE;
    }

    public lookBack(skipHidden: boolean = true) {
        let index = this._index;
        while (index > 0) {
            --index;
            if (this._tokens[index].channel === Token.DEFAULT_CHANNEL || !skipHidden) {
                return this._tokens[index].type;
            }
        }
        
        return Token.INVALID_TYPE;
    }

    public seek(index: number): void {
        if (index < this._tokens.length) {
            this._index = index;
        }
    }
    
    public reset() {
        this._index = 0;
        this._tokenStack.length = 0;
    }
    
    public push() {
        this._tokenStack.push(this._index);
    }
    
    public pop(): boolean {
        if (!this._tokenStack.length) {
            return false;
        }
        
        this._index = this._tokenStack.pop();
        return true;
    }
    
    public removeTos() {
        if (this._tokenStack.length > 0) {
            this._tokenStack.pop();
        }
    }

    public is(type: number): boolean {
        return this._tokens[this._index].type === type;
    }
    
    public tokenText(keepQuotes: boolean = false): string {
        return this._tokens[this._index].text;
    }

    public tokenType(): number {
        return this._tokens[this._index].type;
    }

    public tokenLine(): number {
        return this._tokens[this._index].line;
    }

    public tokenStart(): number {
        return this._tokens[this._index].charPositionInLine;
    }

    public get tokenIndex(): number {
        return this._tokens[this._index].tokenIndex;
    }
    
    public tokenOffset(): number {
        return this._tokens[this._index].startIndex;
    }
    
    public tokenLength(): number {
        const token = this._tokens[this._index];
        return token.stopIndex - token.startIndex + 1;
    }

    public tokenChannel(): number {
        return this._tokens[this._index].channel;
    }
    
    public tokenSubText(): string {
        const cs = this._tokens[this._index].tokenSource.inputStream;
        return cs.getText(Interval.of(this._tokens[this._index].startIndex, Number.MAX_SAFE_INTEGER)); 
    }
}
