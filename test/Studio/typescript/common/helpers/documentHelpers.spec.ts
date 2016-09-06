import helper = require("src/Raven.Studio/typescript/common/helpers/database/documentHelpers");
import document = require("src/Raven.Studio/typescript/models/database/documents/document");
import chai = require("chai");

var helperUnderTest = 'common/helpers/database/documentHelpers';

describe(helperUnderTest, () => {

    it('should find intersection on simple documents', () => {

        var doc1: any = document.empty();
        doc1.name = "John";

        var doc2: any = document.empty();
        doc2.name = "Greg";

        var schema = helper.findSchema([doc1, doc2]);

        chai.expect(schema.toDto(false)).to.deep.equal({ name: "", "@metadata": undefined });
    });

    it('should find intersection on complex object', () => {
        var doc1: any = document.empty();
        doc1.name = "John";
        doc1.address = {
            zipCode: 80123,
            street: "aaa",
            propOnlyInFirst: "321321"
        };

        var doc2: any = document.empty();
        doc2.address = {
            zipCode: 80111,
            street: "bbb",
            propOnlyInSecond: "123123"
        };
        doc2.name = "Greg";      

        var schema = helper.findSchema([doc1, doc2]);

        chai.expect(schema.toDto((false))).to.deep.equal({ name: "", address: { zipCode: 0, street: "" }, "@metadata": undefined });
    });

    it('should infer RavenEntityName and ClrType from metadata', () => {
        var doc1: any = document.empty();
        doc1.name = "John";
        doc1.__metadata.ravenEntityName = "People";
        doc1.__metadata.ravenClrType = "Acme.Models.People";
        doc1.__metadata.anotherProperty = "test";

        var doc2: any = document.empty();
        doc2.name = "Greg";
        doc2.__metadata.ravenEntityName = "People";
        doc2.__metadata.ravenClrType = "Acme.Models.People";
        doc2.__metadata.anotherProperty = "test";

        var schema = helper.findSchema([doc1, doc2]);

        chai.expect(schema.toDto(false)).to.deep.equal({ name: "", "@metadata": undefined });

        chai.expect(schema.__metadata.ravenClrType).to.equal("Acme.Models.People");
        chai.expect(schema.__metadata.ravenEntityName).to.equal("People");
    });

});
