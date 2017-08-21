/// <reference path="../../../../src/Raven.Studio/typings/tsd.d.ts" />

import chai = require("chai");

import rqlTestUtils = require("autocomplete/rqlTestUtils");

const emptyProvider = rqlTestUtils.emptyProvider;
const northWindProvider = rqlTestUtils.northWindProvider;

describe("RQL Autocomplete", () => {
   it('empty query should start with from or select', done => {
       rqlTestUtils.autoComplete("|", emptyProvider(), ((errors, wordlist) => {
          chai.expect(wordlist.map(x => x.value)).to.deep.equal(["from", "select"]);
          
          done();
      }))
   });
   
   it('should autocomplete collection names', done => {
       rqlTestUtils.autoComplete("from |", northWindProvider(),  ((errors, wordlist) => {
           chai.expect(wordlist.map(x => x.value)).to.include("Orders ");

           done();
       }));
   });
});
