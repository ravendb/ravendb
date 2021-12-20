import { CharStreams, CommonTokenStream } from "antlr4ts";
import { ParsedRql } from "./types";
import { ANTLRErrorListener } from "antlr4ts/ANTLRErrorListener";
import { Token } from "antlr4ts/Token";
import { RqlLexer } from "./RqlLexer";
import { RqlParser } from "./RqlParser";

export interface parseRqlOptions {
    onLexerError?: ANTLRErrorListener<Token>["syntaxError"];
    onParserError?: ANTLRErrorListener<Token>["syntaxError"];
}

export function parseRql(input: string, opts: parseRqlOptions = {}): ParsedRql {
    const chars = CharStreams.fromString(input);
    const lexer = new RqlLexer(chars);
    lexer.removeErrorListeners();
    
    const tokenStream = new CommonTokenStream(lexer);
    const parser = new RqlParser(tokenStream);
    parser.buildParseTree = true;
    parser.removeErrorListeners();
    
    if (opts.onLexerError) {
        lexer.addErrorListener({
            syntaxError: opts.onLexerError
        });
    }
    
    if (opts.onParserError) {
        parser.addErrorListener({
            syntaxError: opts.onParserError
        });
    }

    const parseTree = parser.prog();
    
    return {
        parser, 
        parseTree,
        tokenStream
    }
}
