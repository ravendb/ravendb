import { parseRql } from "../../src/parser";
import {
    GetAllDistinctContext, 
    JavascriptCodeContext,
    ProjectIndividualFieldsContext
} from "../../src/generated/BaseRqlParser";

describe("SELECT statement parser", function () {
    it("single", function () {
        const { parseTree, parser } = parseRql("from test order by item as ALPHANUMERIC desc select x");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const select = parseTree.selectStatement();
        
        expect(select)
            .toBeInstanceOf(ProjectIndividualFieldsContext);
        
        expect((select as ProjectIndividualFieldsContext)._field.text)
            .toEqual("x");
    });
    
    it("distinct", function () {
        const { parseTree, parser } = parseRql("from test order by item as ALPHANUMERIC desc select distinct *");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const select = parseTree.selectStatement();

        expect(select)
            .toBeInstanceOf(GetAllDistinctContext);
    });

    it("javascript", function () {
        const { parseTree, parser } = parseRql("from test order by item as ALPHANUMERIC desc select {test;}");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const select = parseTree.selectStatement();

        expect(select)
            .toBeInstanceOf(JavascriptCodeContext);
    });
});
