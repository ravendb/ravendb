import document = require("models/database/documents/document");
import documentHelpers from "common/helpers/database/documentHelpers";

describe("documentHelpers", function () {

    it('should find intersection on simple documents', () => {
        const doc1: any = document.empty();
        doc1.name = "John";

        const doc2: any = document.empty();
        doc2.name = "Greg";

        const schema = documentHelpers.findSchema([doc1, doc2]);

        expect(schema.toDto(false))
            .toMatchObject({ name: "", "@metadata": undefined });
    });
    
    it('should find intersection on complex object', () => {
        const doc1: any = document.empty();
        doc1.name = "John";
        doc1.address = {
            zipCode: 80123,
            street: "aaa",
            propOnlyInFirst: "321321"
        };

        const doc2: any = document.empty();
        doc2.address = {
            zipCode: 80111,
            street: "bbb",
            propOnlyInSecond: "123123"
        };
        doc2.name = "Greg";

        const schema = documentHelpers.findSchema([doc1, doc2]);

        expect(schema.toDto(false)).toMatchObject({ name: "", address: { zipCode: 0, street: "" }, "@metadata": undefined });
    });

    it('should infer @collection and ClrType from metadata', () => {
        const doc1: any = document.empty();
        doc1.name = "John";
        doc1.__metadata.collection = "People";
        doc1.__metadata.ravenClrType = "Acme.Models.People";
        doc1.__metadata.anotherProperty = "test";

        const doc2: any = document.empty();
        doc2.name = "Greg";
        doc2.__metadata.collection = "People";
        doc2.__metadata.ravenClrType = "Acme.Models.People";
        doc2.__metadata.anotherProperty = "test";

        const schema = documentHelpers.findSchema([doc1, doc2]);

        expect(schema.toDto(false)).toMatchObject({ name: "", "@metadata": undefined });

        expect(schema.__metadata.ravenClrType).toEqual("Acme.Models.People");
        expect(schema.__metadata.collection).toEqual("People");
    });
});
