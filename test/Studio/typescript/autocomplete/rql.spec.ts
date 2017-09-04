/// <reference path="../../../../src/Raven.Studio/typings/tsd.d.ts" />

import chai = require("chai");
const assert = chai.assert;

import rqlTestUtils = require("autocomplete/rqlTestUtils");

const emptyProvider = rqlTestUtils.emptyProvider;
const northwindProvider = rqlTestUtils.northwindProvider;

describe("RQL Autocomplete", () => {
    it('empty query should start with from or declare or select', done => {
        rqlTestUtils.autoComplete("|", northwindProvider(), ((errors, wordlist, prefix) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, [
                {caption: "from", value: "from ", score: 2, meta: "keyword"},
                {caption: "declare", value: "declare ", score: 1, meta: "keyword"},
                {caption: "select", value: "select ", score: 0, meta: "keyword"},
            ]);

            done();
        }))
    });

    const collectionsList = [
        {caption: "Regions", value: "Regions ", score: 2, meta: "collection"},
        {caption: "Suppliers", value: "Suppliers ", score: 2, meta: "collection"},
        {caption: "Employees", value: "Employees ", score: 2, meta: "collection"},
        {caption: "Categories", value: "Categories ", score: 2, meta: "collection"},
        {caption: "Products", value: "Products ", score: 2, meta: "collection"},
        {caption: "Shippers", value: "Shippers ", score: 2, meta: "collection"},
        {caption: "Companies", value: "Companies ", score: 2, meta: "collection"},
        {caption: "Orders", value: "Orders ", score: 2, meta: "collection"},
        {caption: "Collection With Space", value: "'Collection With Space' ", score: 2, meta: "collection"},
        {caption: "Collection!", value: "'Collection!' ", score: 2, meta: "collection"},
        //TODO: {caption: "Collection With Space", value: "'Collection With ' And \" in name' ", score: 2, meta: "collection"},
        {caption: "index", value: "index ", score: 4, meta: "keyword"},
        {caption: "@all_docs", value: "@all_docs ", score: 3, meta: "collection"},
        {caption: "@system", value: "@system ", score: 1, meta: "collection"},
    ];
    it('from should get collection names', done => {
        rqlTestUtils.autoComplete("from |", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, collectionsList);

            done();
        }));
    });

    it('from Orders| should not list anything but have the Orders prefix', done => {
        rqlTestUtils.autoComplete("from Orders|", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "Orders");
            assert.deepEqual(wordlist, collectionsList);

            done();
        }));
    });

    it('from Collection | should list collections', done => {
        rqlTestUtils.autoComplete("from Orders |", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "");

            const afterFromList = [
                {caption: "as", value: "as ", score: 7, meta: "keyword"},
                {caption: "where", value: "where ", score: 6, meta: "keyword"},
                {caption: "group", value: "group ", score: 5, meta: "keyword"},
                {caption: "load", value: "load ", score: 4, meta: "keyword"},
                {caption: "select", value: "select ", score: 3, meta: "keyword"},
                {caption: "order", value: "order ", score: 2, meta: "keyword"},
                {caption: "include", value: "include ", score: 1, meta: "keyword"}
            ];
            assert.deepEqual(_.sortBy(wordlist, [(x: autoCompleteWordList) => x.score]).reverse(), afterFromList);

            done();
        }));
    });

    it('from Collection select | should list fields', done => {
        rqlTestUtils.autoComplete("from Orders select |", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, [
                {caption: "Company", value: "Company", score: 1, meta: "string field"},
                {caption: "Employee", value: "Employee", score: 1, meta: "string field"},
                {caption: "OrderedAt", value: "OrderedAt", score: 1, meta: "string field"},
                {caption: "RequireAt", value: "RequireAt", score: 1, meta: "string field"},
                {caption: "ShippedAt", value: "ShippedAt", score: 1, meta: "string field"},
                {caption: "ShipTo", value: "ShipTo", score: 1, meta: "object field"},
                {caption: "ShipVia", value: "ShipVia", score: 1, meta: "string field"},
                {caption: "Freight", value: "Freight", score: 1, meta: "number field"},
                {caption: "Lines", value: "Lines", score: 1, meta: "object[] field"},
                {caption: "@metadata", value: "@metadata", score: 1, meta: "object field"}
            ]);

            done();
        }));
    });

    const indexesList = [
        {caption: "Orders/ByCompany", value: "'Orders/ByCompany' ", score: 1, meta: "index"},
        {caption: "Product/Sales", value: "'Product/Sales' ", score: 1, meta: "index"},
        {caption: "Orders/Totals", value: "'Orders/Totals' ", score: 1, meta: "index"},
    ];
    it('from index should get index names', done => {
        rqlTestUtils.autoComplete("from index |", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, indexesList);

            done();
        }));
    });

    it('from index inside index name should complete with prefix', done => {
        rqlTestUtils.autoComplete("from index 'Orders/Tot|", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "'Orders/Tot");
            assert.deepEqual(wordlist, indexesList);

            done();
        }));
    });

    it('from index after index name without space should complete with prefix', done => {
        rqlTestUtils.autoComplete("from index 'Orders/Totals'|", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "'Orders/Totals'");
            assert.deepEqual(wordlist, indexesList);

            done();
        }));
    });

    it('from index after index name should complete with after from', done => {
        rqlTestUtils.autoComplete("from index 'Orders/Totals' |", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "");

            const afterFromList = [
                {caption: "as", value: "as ", score: 7, meta: "keyword"},
                {caption: "where", value: "where ", score: 6, meta: "keyword"},
                {caption: "load", value: "load ", score: 4, meta: "keyword"},
                {caption: "select", value: "select ", score: 3, meta: "keyword"},
                {caption: "order", value: "order ", score: 2, meta: "keyword"},
                {caption: "include", value: "include ", score: 1, meta: "keyword"}
            ];
            assert.deepEqual(_.sortBy(wordlist, [(x: autoCompleteWordList) => x.score]).reverse(), afterFromList);

            done();
        }));
    });

    it('show fields of index', done => {
        rqlTestUtils.autoComplete("from index 'Orders/Totals' select |", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, [
                {caption: "Employee", value: "Employee", score: 1, meta: "field"},
                {caption: "Company", value: "Company", score: 1, meta: "field"},
                {caption: "Total", value: "Total", score: 1, meta: "field"}
            ]);

            done();
        }));
    });

    it('declare should suggest function', done => {
        rqlTestUtils.autoComplete("declare |", northwindProvider(),  ((errors, wordlist, prefix) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, [
                {caption: "function", value: "function ", score: 0, meta: "keyword"},
            ]);

            done();
        }));
    });
});

