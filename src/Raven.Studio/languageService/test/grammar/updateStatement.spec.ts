import { parseRql } from "../../src/parser";
import {
    UpdateStatementContext
} from "../../src/generated/BaseRqlParser";

describe("UPDATE statement", function () {

    it("doesn't throw when brackets are symmetrical", function () {
        const {
            parseTree,
            parser
        } = parseRql("from Orders update { for (var i = 0; i < 10; i++ ) { put(\"orders/\", this); } }");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const updateStatement = parseTree.updateStatement();

        expect(updateStatement)
            .toBeInstanceOf(UpdateStatementContext);
    });

    it("doesn't throw with JSON inside", function () {
        const {
            parseTree,
            parser
        } = parseRql("from test where id() == 't' update { for(var i = 0; i< this.characters.length; i++) { var cur = this.characters[i]; cur[\"@metadata\"] = {\"@collection\": \"Characters\" }; put(cur.name, cur); } } ");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const updateStatement = parseTree.updateStatement();

        expect(updateStatement)
            .toBeInstanceOf(UpdateStatementContext);
    });

    it("throws when brackets aren't symmetrical", function () {
        const {
            parseTree,
            parser
        } = parseRql("from Orders update { for (var i = 0; i < 10; i++ ) { put(\"orders/\", this); } }" +
            "}"); // <-- here is extra bracket

        expect(parser.numberOfSyntaxErrors)
            .toEqual(1);

        const update = parseTree.updateStatement();

        expect(update)
            .toBeInstanceOf(UpdateStatementContext);
    });
});
