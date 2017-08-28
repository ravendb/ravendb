/// <reference path="../../../../../src/Raven.Studio/typings/globals/mocha/index.d.ts" />
/// <reference path="../../../../../src/Raven.Studio/typings/globals/chai/index.d.ts" />

import helper = require("src/Raven.Studio/typescript/common/helpers/database/documentHelpers");
import document = require("src/Raven.Studio/typescript/models/database/documents/document");
import chai = require("chai");

const helperUnderTest = 'common/helpers/database/documentHelpers';

describe(helperUnderTest, () => {

    it('should find intersection on simple documents', () => {

        const doc1: any = document.empty();
        doc1.name = "John";

        const doc2: any = document.empty();
        doc2.name = "Greg";

        const schema = helper.findSchema([doc1, doc2]);

        chai.expect(schema.toDto(false)).to.deep.equal({ name: "", "@metadata": undefined });
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

        const schema = helper.findSchema([doc1, doc2]);

        chai.expect(schema.toDto((false))).to.deep.equal({ name: "", address: { zipCode: 0, street: "" }, "@metadata": undefined });
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

        const schema = helper.findSchema([doc1, doc2]);

        chai.expect(schema.toDto(false)).to.deep.equal({ name: "", "@metadata": undefined });

        chai.expect(schema.__metadata.ravenClrType).to.equal("Acme.Models.People");
        chai.expect(schema.__metadata.collection).to.equal("People");
    });

});
