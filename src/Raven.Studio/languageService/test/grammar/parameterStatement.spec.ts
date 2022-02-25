import { parseRql } from "../../src/parser";
import { ParameterBeforeQueryContext } from "../../src/generated/BaseRqlParser";

describe("Parameter statement parser", function () {
    it("uncompleted definition of parameter", function () {
        const { parser } = parseRql("$p0 = from test filter x");

        expect(parser.numberOfSyntaxErrors)
            .toBeGreaterThan(0);
    });

    it("before query", function () {
        const { parseTree, parser } = parseRql("$test = 'p0' $p0=1 from test");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
        
        const paramBeforeRql = parseTree.parameterBeforeQuery();
        expect(paramBeforeRql)
            .toBeInstanceOf(Array);
        const expr = paramBeforeRql as ParameterBeforeQueryContext[];
        expect(expr.length)
            .toEqual(2);
        
        expect(expr[0].literal().text)
            .toEqual("$test")
        expect(expr[0].parameterValue().text)
            .toBe("'p0'");

        expect(expr[1].literal().text)
            .toEqual("$p0")
        expect(expr[1].parameterValue().text)
            .toBe("1");
    });

    it("JSON without bracelets throws", function () {
        const { parser } = parseRql("from test 7");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);
    });

    it("Json object", function () {
        const { parser } = parseRql("from Employees {\"test\": 1, \"object\": { \"inside\": [1,\"test\"] }, \"string\": \"works\"}");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });
});
