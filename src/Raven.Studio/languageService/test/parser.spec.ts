import { CharStreams, CommonTokenStream } from "antlr4ts";
import { RqlLexer } from "../src/generated/RqlLexer";
import { RqlParser } from "../src/generated/RqlParser";
import { CodeCompletionCore } from "antlr4-c3";

const queries = require("./data/queries.json");

describe("parser", function () {
    for (const query of queries) {
        it("can parse - " + query, () => {
            const chars = CharStreams.fromString(query);
            const lexer = new RqlLexer(chars);
            const tokens = new CommonTokenStream(lexer);
            const parser = new RqlParser(tokens);   
            parser.buildParseTree = true;

            parser.prog();
            
            expect(parser.numberOfSyntaxErrors)
                .toEqual(0);

            const core = new CodeCompletionCore(parser);
            
            core.collectCandidates(tokens.size);
        });
    }
})
