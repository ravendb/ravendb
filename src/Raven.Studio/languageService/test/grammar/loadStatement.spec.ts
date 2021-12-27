import { parseRql } from "../../src/parser";
import {
    LoadDocumentByNameContext,
    LoadStatementContext
} from "../../src/generated/BaseRqlParser";

describe("LOAD statement parser", function() {
    it("single", function() {
        const { parseTree, parser } = parseRql("from test load x as y");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const load = parseTree.loadStatement();
        expect(load)
            .toBeInstanceOf(LoadStatementContext);
        expect(load._item._name.text)
            .toEqual("x");
        
        expect(load._item._as.text).toEqual("asy");
    });

    it("array", function() {
        const { parseTree, parser } = parseRql("from test load itemA as aliasA, itemB as aliasB");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
        const load = parseTree.loadStatement();
        expect(load)
            .toBeInstanceOf(LoadStatementContext);
       
        expect(load.children[1])
            .toBeInstanceOf(LoadDocumentByNameContext);
        const itemA = load.children[1] as LoadDocumentByNameContext;
        expect(itemA._name.text)
            .toEqual("itemA");
        expect(itemA._as._name.text)
            .toEqual("aliasA");

        expect(load.children[3])
            .toBeInstanceOf(LoadDocumentByNameContext);
        const itemB = load.children[3] as LoadDocumentByNameContext;
        expect(itemB._name.text)
            .toEqual("itemB");
        expect(itemB._as._name.text)
            .toEqual("aliasB");
    });

    it("load with reduce", function() {
        const { parseTree, parser } = parseRql("from Orders as o\n" +
            "group by Company\n" +
            "order by Sum as long desc\n" +
            "load Company as c\n" +
            "select sum(o.Lines[].Quantity) as Sum, c.Name");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
    });
});
