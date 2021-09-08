import { CharStreams, CommonTokenStream } from "antlr4ts";
import { RqlLexer } from "./generated/RqlLexer";
import { RqlParser } from "./generated/RqlParser";
import { ParsedRql } from "./types";
import { Recognizer } from "antlr4ts/Recognizer";
import { RecognitionException } from "antlr4ts/RecognitionException";
import { Token } from "antlr4ts/Token";

interface parseRqlOptions {
    onSyntaxError?<T>(
        recognizer: Recognizer<T, any>, 
        offendingSymbol: T, 
        line: number, 
        charPositionInLine: number, 
        msg: string, 
        e: RecognitionException
    ): void;
}

export function parseRql(input: string, opts: parseRqlOptions = {}): ParsedRql {
    const chars = CharStreams.fromString(input);
    const lexer = new RqlLexer(chars);
    lexer.removeErrorListeners();
    
    const tokenStream = new CommonTokenStream(lexer);
    const parser = new RqlParser(tokenStream);
    parser.buildParseTree = true;
    parser.removeErrorListeners();
    
    if (opts.onSyntaxError) {
        parser.addErrorListener({
            syntaxError: opts.onSyntaxError
        });
        lexer.addErrorListener({
            syntaxError: opts.onSyntaxError
        });
    }

    const parseTree = parser.prog();
    
    return {
        parser, 
        parseTree,
        tokenStream
    }
}
