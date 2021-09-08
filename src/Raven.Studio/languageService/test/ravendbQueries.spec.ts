import { CodeCompletionCore } from "antlr4-c3";
import { parseRql } from "../src/parser";

const queries = require("./data/queries.json");

const isTimeSeriesTest = (name: string) => name.includes("declare timeseries ") || name.includes("timeseries("); 
const graphQuery = (name: string) => name.trim().startsWith("match") || name.trim().startsWith("with");

describe("RavenDB Queries", function () {
    for (const query of queries) {
        if (isTimeSeriesTest(query) || graphQuery(query)) {
            it.skip("can parse - " + query, () => {});
        } else {
            it("can parse - " + query, () => {
                const errors: string[] = [];
                
                const { parser, tokenStream } = parseRql(query, {
                    onSyntaxError: (recognizer, offendingSymbol, line, charPositionInLine, msg) => errors.push(msg)
                });

                expect(parser.numberOfSyntaxErrors)
                    .toEqual(0);
                
                expect(errors.length)
                    .toEqual(0);
                
                const core = new CodeCompletionCore(parser);
                core.collectCandidates(tokenStream.size);
            });
        }
    }
})
