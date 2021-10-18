import {parseRql} from "../../src/parser";
import {
    BetweenExprContext,
    BinaryExpressionContext, CollectionByNameContext,
    EqualExpressionContext, FunctionContext, NormalFuncContext,
} from "../../src/generated/BaseRqlParser";

describe("WHERE statement parser", function () {
    it("single literal", function () {
        const { parser } = parseRql("from test where x");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);
    });

    it("comma", function () {
        const { parser } = parseRql("from test where x = y,");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);
    });

    it("true and exist", function () {
        const { parser } = parseRql("from test where true and exist(x)");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });

    it("binary and all in()", function () {
        const { parser } = parseRql("from test where x=1 and y all in (1,2,3)");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });
    
    it("equal", function () {
        const { parseTree, parser } = parseRql("from test where x = 5");

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
        const { parseTree, parser } = parseRql("from test where x = 5 and y between 1 and 100");

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
    
    it("can't use `where` as from alias", function () {
        const { parseTree, parser } = parseRql("from test where");

        expect(parser.numberOfSyntaxErrors)
            .toBeGreaterThanOrEqual(1);
        
        const from = parseTree.fromStatement();
        expect(from)
            .toBeInstanceOf(CollectionByNameContext);
        const collectionByName = from as CollectionByNameContext;
        expect(collectionByName.aliasWithOptionalAs())
            .toBeFalsy();

        const where = parseTree.whereStatement();
        expect(where)
            .toBeTruthy();
    });

        it("parsing function", function () {
           const { parseTree, parser } = parseRql("from test where first.second.third(argument)");
           
           expect(parser.numberOfSyntaxErrors)
               .toEqual(0);
           
           const where = parseTree.whereStatement();
            const expr = where.expr();
            expect(expr)
                .toBeInstanceOf(NormalFuncContext);
            const func = expr as NormalFuncContext;
            
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
    
});
