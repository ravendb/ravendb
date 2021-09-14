import {parseRql} from "../../src/parser";
import {
    GroupByStatementContext,
    ParameterWithOptionalAliasContext,
} from "../../src/generated/RqlParser";

describe("GROUP BY statement parser", function () {
    it("single", function () {
        const {parseTree, parser} = parseRql("from test group by zebra");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const groupBy = parseTree.groupByStatement();
        expect(groupBy)
            .toBeInstanceOf(GroupByStatementContext);
        
        const value = groupBy._value;
        expect(value)
            .toBeInstanceOf(ParameterWithOptionalAliasContext);
        
        expect(value.text).toEqual("zebra");
    });

    it("with alias", function () {
        const {parseTree, parser} = parseRql("from test group by zebra as Double");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const groupBy = parseTree.groupByStatement();
        expect(groupBy)
            .toBeInstanceOf(GroupByStatementContext);

        const value = groupBy._value;
        expect(value)
            .toBeInstanceOf(ParameterWithOptionalAliasContext);

        expect(value._value.text).toEqual("zebra");
        expect(value._as.text).toEqual("asDouble")
    });
    
    it("unexpected comma before alias", function () {
        const {parseTree, parser} = parseRql("from test group by zebra, as Double");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);
    });
});
