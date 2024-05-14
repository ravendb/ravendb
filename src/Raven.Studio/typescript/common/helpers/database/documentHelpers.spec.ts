import document = require("models/database/documents/document");
import documentHelpers from "common/helpers/database/documentHelpers";

function assertFindRelatedDocumentMatch(fieldValue: string) {
    const doc1: any = document.empty();
    doc1.addressId = fieldValue;

    const candidates = documentHelpers.findRelatedDocumentsCandidates(doc1);
    expect(candidates)
        .toHaveLength(1);
    expect(candidates)
        .toContain(fieldValue);
}

function assertFindRelatedDocumentDoesMatch(fieldValue: any) {
    const doc1: any = document.empty();
    doc1.addressId = fieldValue;

    const candidates = documentHelpers.findRelatedDocumentsCandidates(doc1);
    expect(candidates)
        .toHaveLength(0);
}

describe("documentHelpers", function () {
    
    describe("findRelatedDocumentsCandidates", function () {
        it("can find related document by collectionName/number", () => {
            assertFindRelatedDocumentMatch("Addresses/1");
        });

        it("can find related document by collectionName/number-TAG", () => {
            assertFindRelatedDocumentMatch("Addresses/1-A");
        });

        it("can find related document by collectionName/GUID-with-dashes", () => {
            assertFindRelatedDocumentMatch("Addresses/4b5934f9-e937-4690-abcb-17b9681cdf35");
        });

        it("can find related document by collectionName/GUID-without-dashes", () => {
            assertFindRelatedDocumentMatch("Addresses/4b5934f9e9374690abcb17b9681cdf35");
        });

        it("can find related document by GUID-with-dashes", () => {
            assertFindRelatedDocumentMatch("4b5934f9-e937-4690-abcb-17b9681cdf35");
        });

        it("can find related document by GUID-without-dashes", () => {
            assertFindRelatedDocumentMatch("4b5934f9e9374690abcb17b9681cdf35");
        });
        
        it("doesn't find candidate in regular string", () => {
            assertFindRelatedDocumentDoesMatch("John");
        })

        it("doesn't find candidate in regular number", () => {
            assertFindRelatedDocumentDoesMatch(56);
        })
    });
    

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
