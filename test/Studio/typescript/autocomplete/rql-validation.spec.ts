/// <reference path="../../../../src/Raven.Studio/typings/tsd.d.ts" />

import chai = require("chai");
const assert = chai.assert;

import rqlTestUtils = require("autocomplete/rqlTestUtils");

describe("RQL Validator", () => {
    

    it('DEMO test case', done => {
        rqlTestUtils.validationTest(`

declare function Name2() {
    var a = 
}

from Orders as o`, (annotations: Array<AceAjax.Annotation>) => {
            
            assert.equal(2, annotations.length);
            
            done();
        })
    });
});

