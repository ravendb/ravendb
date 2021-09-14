import {parseRql} from "../../src/parser";
import {
    LoadDocumentByNameContext,
    LoadStatementContext
} from "../../src/generated/RqlParser";

describe("LOAD statement parser", function () {
    it("single", function () {
        const {parseTree, parser} = parseRql("from test load x as y");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const load = parseTree.loadStatement();
        expect(load)
            .toBeInstanceOf(LoadStatementContext);
        expect(load._item._name.text)
            .toEqual("x");
        
        expect(load._item._as.text).toEqual("asy");
    });

    it("array", function () {
        const {parseTree, parser} = parseRql("from test load t as t, t as t");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
        const load = parseTree.loadStatement();
        expect(load)
            .toBeInstanceOf(LoadStatementContext);
        // expect(load).con
        for (let index = 0; index < load.children.length; index++) {
            if (index % 2 == 0)
                continue;

            expect(load.children[index])
                .toBeInstanceOf(LoadDocumentByNameContext);
            const item = load.children[index] as LoadDocumentByNameContext;
            expect(item._name.text)
                .toEqual("t");
            expect(item._as.text)
                .toEqual("ast");
        }
            
        // for(i = 0; i < load.childCount; ++i)
        // {
        //    
        // }
        // expect(load._item._name.text)
        //     .toEqual("x");
        //
        // expect(load._item._as.text).toEqual("asy");
    });
});
