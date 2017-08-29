/// <reference path="../../../../src/Raven.Studio/typings/tsd.d.ts" />

import chai = require("chai");
const assert = chai.assert;

import rqlTestUtils = require("autocomplete/rqlTestUtils");

const emptyProvider = rqlTestUtils.emptyProvider;
const northwindProvider = rqlTestUtils.northwindProvider;

describe("RQL Autocomplete", () => {
   it('empty query should start with from or declare or select', done => {
       rqlTestUtils.autoComplete("|", northwindProvider(), ((errors, wordlist) => {
           assert.deepEqual(wordlist, [
               {caption: "from", value: "from ", score: 2, meta: "keyword"},
               {caption: "declare", value: "declare ", score: 1, meta: "keyword"},
               {caption: "select", value: "select ", score: 0, meta: "keyword"},
           ]);
            
           done();
      }))
   });
   
   it('from should get collection names', done => {
       rqlTestUtils.autoComplete("from |", northwindProvider(),  ((errors, wordlist) => {
           assert.deepEqual(wordlist, [
                {caption: "Regions", value: "Regions ", score: 2, meta: "collection"},
                {caption: "Suppliers", value: "Suppliers ", score: 2, meta: "collection"},
                {caption: "Employees", value: "Employees ", score: 2, meta: "collection"},
                {caption: "Categories", value: "Categories ", score: 2, meta: "collection"},
                {caption: "Products", value: "Products ", score: 2, meta: "collection"},
                {caption: "Shippers", value: "Shippers ", score: 2, meta: "collection"},
                {caption: "Companies", value: "Companies ", score: 2, meta: "collection"},
                {caption: "Orders", value: "Orders ", score: 2, meta: "collection"},
                {caption: "index", value: "index ", score: 4, meta: "keyword"},
                {caption: "@all_docs", value: "@all_docs ", score: 3, meta: "collection"},
                {caption: "@system", value: "@system ", score: 1, meta: "collection"},
           ]);

           done();
       }));
   });
   
   it('declare should suggest function', done => {
       rqlTestUtils.autoComplete("declare |", northwindProvider(),  ((errors, wordlist) => {
           assert.deepEqual(wordlist, [
                {caption: "function", value: "function ", score: 0, meta: "keyword"},
           ]);

           done();
       }));
   });
});

