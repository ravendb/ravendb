import { CodeCompletionCore } from "antlr4-c3";
import { parseRql } from "../src/parser";

const queries = require("./data/queries.json");

describe("RavenDB Queries", function () {
    for (const query of queries) {
        it("can parse - " + query, () => {
            const { parser, tokenStream } = parseRql(query);
            
            expect(parser.numberOfSyntaxErrors)
                .toEqual(0);

            const core = new CodeCompletionCore(parser);
            core.collectCandidates(tokenStream.size);
        });
    }
})
