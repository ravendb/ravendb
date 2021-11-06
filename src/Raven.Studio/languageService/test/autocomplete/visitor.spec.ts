import { RqlQueryVisitor } from "../../src/rqlQueryVisitor";
import { parseRql } from "../../src/parser";


describe("RQL Query Visitor", function () {
    it("can extract alias from collection - w/o as", async function() {
        const { parseTree } = parseRql("from Orders oo");
        
        const result = new RqlQueryVisitor("Select").visit(parseTree);
        expect(result.fromAlias)
            .toEqual("oo");
    });

    it("can extract alias from collection - w as", async function() {
        const { parseTree } = parseRql("from Orders as oo");

        const result = new RqlQueryVisitor("Select").visit(parseTree);
        expect(result.fromAlias)
            .toEqual("oo");
    });
    
    it("can extract alias from index - w/o as", async function() {
        const { parseTree } = parseRql("from index 'Orders/Total' ot");

        const result = new RqlQueryVisitor("Select").visit(parseTree);
        expect(result.fromAlias)
            .toEqual("ot");
    });

    it("can extract alias from index - w/ as", async function() {
        const { parseTree } = parseRql("from index 'Orders/Total' as ot");

        const result = new RqlQueryVisitor("Select").visit(parseTree);
        expect(result.fromAlias)
            .toEqual("ot");
    });
    
    it("doesn't throw when no from alias - index", async function() {
        const { parseTree } = parseRql("from index 'Orders/Total'");

        new RqlQueryVisitor("Select").visit(parseTree);
    });

    it("doesn't throw when no from alias - collection", async function() {
        const { parseTree } = parseRql("from Orders");

        new RqlQueryVisitor("Select").visit(parseTree);
    });
});
