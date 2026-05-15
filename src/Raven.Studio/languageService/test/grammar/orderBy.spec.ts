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

    it("with NULLS FIRST", function () {
        const { parseTree, parser } = parseRql("from test order by item NULLS FIRST");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const item = parseTree.orderByStatement()._value;
        expect(item._value.text).toEqual("item");
        expect(item._nullsValue.text.toLowerCase()).toEqual("nulls first");
    });

    it("with NULLS LAST", function () {
        const { parseTree, parser } = parseRql("from test order by item NULLS LAST");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const item = parseTree.orderByStatement()._value;
        expect(item._nullsValue.text.toLowerCase()).toEqual("nulls last");
    });

    it("nulls clause is case insensitive", function () {
        const { parseTree, parser } = parseRql("from test order by item nulls first");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const item = parseTree.orderByStatement()._value;
        expect(item._nullsValue).toBeDefined();
    });

    it("with AS <type> and NULLS FIRST", function () {
        const { parseTree, parser } = parseRql("from test order by item as long NULLS FIRST");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const item = parseTree.orderByStatement()._value;
        expect(item._order._sortingMode.text).toEqual("long");
        expect(item._nullsValue.text.toLowerCase()).toEqual("nulls first");
    });

    it("with AS <type> DESC and NULLS LAST", function () {
        const { parseTree, parser } = parseRql("from test order by item as long DESC NULLS LAST");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const item = parseTree.orderByStatement()._value;
        expect(item._order._sortingMode.text).toEqual("long");
        expect(item._orderValueType.text.toLowerCase()).toEqual("desc");
        expect(item._nullsValue.text.toLowerCase()).toEqual("nulls last");
    });

    it("per-clause control on multi-key sort", function () {
        const { parseTree, parser } = parseRql(
            "from test order by Name NULLS FIRST, Age DESC, Score AS double NULLS LAST"
        );

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const orderBy = parseTree.orderByStatement();
        const items = orderBy.orderByItem();

        expect(items.length).toEqual(3);
        expect(items[0]._nullsValue.text.toLowerCase()).toEqual("nulls first");
        expect(items[1]._nullsValue).toBeUndefined();
        expect(items[2]._nullsValue.text.toLowerCase()).toEqual("nulls last");
    });

    it("nulls keyword alone is parsed as a field name (no nulls clause)", function () {
        const { parseTree, parser } = parseRql("from test order by nulls");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const item = parseTree.orderByStatement()._value;
        expect(item._value.text).toEqual("nulls");
        expect(item._nullsValue).toBeUndefined();
    });
});
