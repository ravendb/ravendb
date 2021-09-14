import { parseRql } from "../../src/parser";
import { CollectionByIndexContext, CollectionByNameContext } from "../../src/generated/RqlParser";


describe("FROM statement parser", function() {
    it("from collection", function() {
        const { parseTree, parser } = parseRql("from Orders");
        
        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);
        
        const from = parseTree.fromStatement();
        
        expect(from)
            .toBeInstanceOf(CollectionByNameContext);
        
        const collectionByName = from as CollectionByNameContext;
        expect(collectionByName.collectionName().text)
            .toEqual("Orders");
    });

    it("from index (w/o quotes)", function() {
        const { parseTree, parser } = parseRql("from index Index1");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const from = parseTree.fromStatement();

        expect(from)
            .toBeInstanceOf(CollectionByIndexContext);

        const indexByName = from as CollectionByIndexContext;
        expect(indexByName._collection.text)
            .toEqual("Index1");
    });

    it("from index (w/ quotes)", function() {
        const { parseTree, parser } = parseRql(`from index "Orders/ByName"`);

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const from = parseTree.fromStatement();

        expect(from)
            .toBeInstanceOf(CollectionByIndexContext);

        const collectionByName = from as CollectionByIndexContext;
        expect(collectionByName._collection.text)
            .toEqual(`"Orders/ByName"`);
    });

    it("from index (w/o collection)", function() {
        const { parseTree, parser } = parseRql(`from index"`);

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);

        const from = parseTree.fromStatement();

        expect(from)
            .toBeInstanceOf(CollectionByIndexContext);
    });

    it("from index '", function() {
        const { parseTree, parser } = parseRql(`from index '`);

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);

        const from = parseTree.fromStatement();

        expect(from)
        .toBeInstanceOf(CollectionByIndexContext);
    });

    it("from index d", function() {
        const { parseTree, parser } = parseRql(`from index d"`);

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);

        const from = parseTree.fromStatement();

        expect(from)
            .toBeInstanceOf(CollectionByIndexContext);
    });
});
