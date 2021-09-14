import {parseRql} from "../../src/parser";
import {
    BetweenExprContext,
    BinaryExpressionContext,
    EqualExpressionContext,
    ExprContext, ExprValueContext,
    WhereStatementContext
} from "../../src/generated/RqlParser";

describe("WHERE statement parser", function () {
    it("signle literal", function () {
        const {parseTree, parser} = parseRql("from test where x");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);
    });

    it("comma", function () {
        const {parseTree, parser} = parseRql("from test where x = y,");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);
    });

    it("true and exist", function () {
        const {parseTree, parser} = parseRql("from test where true and exist(x)");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });

    it("binary and all in()", function () {
        const {parseTree, parser} = parseRql("from test where x=1 and y all in (1,2,3)");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });
    
    it("equal", function () {
        const {parseTree, parser} = parseRql("from test where x = 5");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const where = parseTree.whereStatement();

        const expr = where.expr();
        expect(expr)
            .toBeInstanceOf(EqualExpressionContext);
        const equalExpression = expr as EqualExpressionContext;

        expect(equalExpression._left.text)
            .toEqual("x");
        expect(equalExpression._right.text)
            .toEqual("5");
    });
    
    it("binary and between", function () {
        const {parseTree, parser} = parseRql("from test where x = 5 and y between 1 and 100");

        expect(parser.numberOfSyntaxErrors)
             .toEqual(0);

        const where = parseTree.whereStatement();

        const expr = where.expr();
        expect(expr)
            .toBeInstanceOf(BinaryExpressionContext);
        const andExpr = expr as BinaryExpressionContext;

        const leftExpr = andExpr._left;
        expect(leftExpr)
            .toBeInstanceOf(EqualExpressionContext);
        const leftExprEqual = andExpr._left as EqualExpressionContext;
        expect(leftExprEqual._left.text)
            .toEqual("x");
        expect(leftExprEqual._right.text)
            .toEqual("5");

        const rightExpr = andExpr._right;
        expect(rightExpr)
            .toBeInstanceOf(BetweenExprContext);
        const betweenExpr = andExpr._right as BetweenExprContext;
        
        const betweenFunction = (betweenExpr as BetweenExprContext).betweenFunction();
        expect(betweenFunction._value.text)
            .toEqual("y")
        expect(betweenFunction._from.text)
            .toEqual("1")
        expect(betweenFunction._to.text)
            .toEqual("100")
    });

});
