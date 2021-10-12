import { parseRql } from "../../src/parser";
import {
    OrderByItemContext,
    OrderByStatementContext
} from "../../src/generated/BaseRqlParser";

describe("ORDER BY statement parser", function () {
    it("single", function () {
        const { parseTree, parser } = parseRql("from test order by item as ALPHANUMERIC desc");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const orderBy = parseTree.orderByStatement();
        
        expect(orderBy)
            .toBeInstanceOf(OrderByStatementContext);
        expect(orderBy._value)
            .toBeInstanceOf(OrderByItemContext);
        
        const item = orderBy._value;
        expect(item._value.text)
            .toEqual("item");
        
        expect(item._order._sortingMode.text)
            .toEqual("ALPHANUMERIC");
        
        expect(item._orderValueType.text)
            .toEqual("desc");
    });
});
