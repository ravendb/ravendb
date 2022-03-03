import { parseRql } from "../../src/parser";
import {
    CollectionByNameContext,
    FilterBinaryExpressionContext,
    FilterEqualExpressionContext, FilterNormalFuncContext,
    FunctionContext,
} from "../../src/generated/BaseRqlParser";

describe("Filter statement parser", function () {
    it("single literal", function () {
        const { parser } = parseRql("from test filter x");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);
    });

    it("comma", function () {
        const { parser } = parseRql("from test filter x = y,");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);
    });

    it("true and exist and filter", function () {
        const { parser } = parseRql("from test where true and exist(x) filter p < 0");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });

    it("spatial", function () {
        const { parser } = parseRql("from Employees filter spatial.within(spatial.point(Location.Latitude, Location.Longitude), spatial.wkt($wkt))");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });

    it("equal", function () {
        const { parseTree, parser } = parseRql("from test filter x = 5");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const filter = parseTree.filterStatement();

        const expr = filter.filterExpr();
        expect(expr)
            .toBeInstanceOf(FilterEqualExpressionContext);
        const equalExpression = expr as FilterEqualExpressionContext;

        expect(equalExpression._left.text)
            .toEqual("x");
        expect(equalExpression._right.text)
            .toEqual("5");
    });

    it("binary and function", function () {
        const { parseTree, parser } = parseRql("from test filter x = 5 and function(y)");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const where = parseTree.filterStatement();

        const expr = where.filterExpr();
        expect(expr)
            .toBeInstanceOf(FilterBinaryExpressionContext);
        const andExpr = expr as FilterBinaryExpressionContext;

        const leftExpr = andExpr._left;
        expect(leftExpr)
            .toBeInstanceOf(FilterEqualExpressionContext);
        const leftExprEqual = andExpr._left as FilterEqualExpressionContext;
        expect(leftExprEqual._left.text)
            .toEqual("x");
        expect(leftExprEqual._right.text)
            .toEqual("5");

        const rightExpr = andExpr._right;
        expect(rightExpr)
            .toBeInstanceOf(FilterNormalFuncContext);
        const normalFunc = andExpr._right as FilterNormalFuncContext;
        const func = (normalFunc as FilterNormalFuncContext).function();
        expect(func._addr.text)
            .toEqual("function");
        expect(func._args.text)
            .toEqual("y");
    });

    it("can't use `filter` as from alias", function () {
        const { parseTree, parser } = parseRql("from test filter");

        expect(parser.numberOfSyntaxErrors)
            .toBeGreaterThanOrEqual(1);

        const from = parseTree.fromStatement();
        expect(from)
            .toBeInstanceOf(CollectionByNameContext);
        const collectionByName = from as CollectionByNameContext;
        expect(collectionByName.aliasWithOptionalAs())
            .toBeFalsy();

        const filter = parseTree.filterStatement();
        expect(filter)
            .toBeTruthy();
    });

    it("parsing function", function () {
        const { parseTree, parser } = parseRql("from test filter first.second.third(argument)");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const filter = parseTree.filterStatement();
        const expr = filter.filterExpr();
        expect(expr)
            .toBeInstanceOf(FilterNormalFuncContext);
        const func = expr as FilterNormalFuncContext;

        const firstMember = (func._funcExpr as FunctionContext)._addr;
        expect(firstMember._name.text)
            .toEqual("first");

        const secondMember = firstMember._member;
        expect(secondMember._name.text)
            .toEqual("second");

        const thirdMember = secondMember._member;
        expect(thirdMember._name.text)
            .toEqual("third");

        expect((func._funcExpr as FunctionContext)._args.text)
            .toEqual("argument");
    });

    it("binary and function with filterLimit", function () {
        const { parser } = parseRql("from test filter x = 5 and function(y) limit 1 filter_limit 1 ");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });

    it("binary and function with only filterLimit", function () {
        const { parser } = parseRql("from test filter x = 5 and function(y) filter_limit 1 ");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });

    it("binary and function with limit and the end", function () {
        const { parser } = parseRql("from test filter x = 5 and function(y) filter_limit 1 limit 1 ");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);
    });
});
