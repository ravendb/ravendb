/// <reference path="../../../../src/Raven.Studio/typings/tsd.d.ts" />

import chai = require("chai");
const assert = chai.assert;

import rqlTestUtils = require("autocomplete/rqlTestUtils");

const emptyProvider = rqlTestUtils.emptyProvider;
const northwindProvider = rqlTestUtils.northwindProvider;

describe("RQL Autocomplete", () => {

    const emptyList = [
        {caption: "from", value: "from ", score: 2, meta: "keyword"},
        {caption: "declare", value: "declare ", score: 1, meta: "keyword"}
    ];

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
        {caption: "Collection With ' And \" in name", value: "'Collection With '' And \" in name' ", score: 2, meta: "collection"},
        {caption: "index", value: "index ", score: 4, meta: "keyword"},
        {caption: "@all_docs", value: "@all_docs ", score: 3, meta: "collection"}
    ];

    const indexesList = [
        {caption: "Orders/ByCompany", value: "'Orders/ByCompany' ", score: 1, meta: "index"},
        {caption: "Product/Sales", value: "'Product/Sales' ", score: 1, meta: "index"},
        {caption: "Orders/Totals", value: "'Orders/Totals' ", score: 1, meta: "index"},
        {caption: "Index With ' And \" in name", value: "'Index With '' And \" in name' ", score: 1, meta: "index"},
    ];

    const fieldsList = [
        {caption: "Company", value: "Company", score: 1, meta: "string field"},
        {caption: "Employee", value: "Employee", score: 1, meta: "string field"},
        {caption: "OrderedAt", value: "OrderedAt", score: 1, meta: "string field"},
        {caption: "RequireAt", value: "RequireAt", score: 1, meta: "string field"},
        {caption: "ShippedAt", value: "ShippedAt", score: 1, meta: "string field"},
        {caption: "ShipTo", value: "ShipTo", score: 1, meta: "object field"},
        {caption: "ShipVia", value: "ShipVia", score: 1, meta: "string field"},
        {caption: "Freight", value: "Freight", score: 1, meta: "number field"},
        {caption: "Lines", value: "Lines", score: 1, meta: "object[] field"},
        {caption: "With.Dot", value: "'With.Dot'", score: 1, meta: "object field"},
        {caption: "With*Star", value: "'With*Star'", score: 1, meta: "object field"},
        {caption: "With Space", value: "'With Space'", score: 1, meta: "object field"},
        {caption: "With ' and \" quotes", value: "'With '' and \" quotes'", score: 1, meta: "object field"},
        {caption: "@metadata", value: "@metadata", score: 1, meta: "object field"}
    ];

    const fieldsShipToList = [
        {caption: "Line1", value: "Line1", score: 1, meta: "string field"},
        {caption: "Line2", value: "Line2", score: 1, meta: "null field"},
        {caption: "City", value: "City", score: 1, meta: "string field"},
        {caption: "Region", value: "Region", score: 1, meta: "string field"},
        {caption: "PostalCode", value: "PostalCode", score: 1, meta: "string field"},
        {caption: "Country", value: "Country", score: 1, meta: "string field"}
    ];

    const afterFromList = [
        {caption: "as", value: "as ", score: 21, meta: "keyword"},
        {caption: "group", value: "group ", score: 20, meta: "keyword"},
        {caption: "where", value: "where ", score: 19, meta: "keyword"},
        {caption: "order", value: "order ", score: 18, meta: "keyword"},
        {caption: "load", value: "load ", score: 17, meta: "keyword"},
        {caption: "select", value: "select ", score: 16, meta: "keyword"},
        {caption: "include", value: "include ", score: 15, meta: "keyword"}
    ];

    const afterFromIndexList = [
        {caption: "as", value: "as ", score: 21, meta: "keyword"},
        {caption: "where", value: "where ", score: 20, meta: "keyword"},
        {caption: "order", value: "order ", score: 19, meta: "keyword"},
        {caption: "load", value: "load ", score: 18, meta: "keyword"},
        {caption: "select", value: "select ", score: 17, meta: "keyword"},
        {caption: "include", value: "include ", score: 16, meta: "keyword"}
    ];

    const afterWhereList = [
        {caption: "and", value: "and ", score: 22, meta: "binary operation"},
        {caption: "or", value: "or ", score: 21, meta: "binary operation"},
        {caption: "order", value: "order ", score: 20, meta: "keyword"},
        {caption: "load", value: "load ", score: 19, meta: "keyword"},
        {caption: "select", value: "select ", score: 18, meta: "keyword"},
        {caption: "include", value: "include ", score: 17, meta: "keyword"}
    ];

    const afterWhereWithoutSpaceList = [
        {caption: "where", value: "where ", score: 20, meta: "keyword"},
        {caption: "order", value: "order ", score: 19, meta: "keyword"},
        {caption: "load", value: "load ", score: 18, meta: "keyword"},
        {caption: "select", value: "select ", score: 17, meta: "keyword"},
        {caption: "include", value: "include ", score: 16, meta: "keyword"}
    ];
    
    it('empty query should start with from or declare', done => {
        rqlTestUtils.autoComplete("|", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, emptyList);

            assert.isNull(lastKeyword);
            
            done();
        })
    });
    
    it('from without space should complete the from itself', done => {
        rqlTestUtils.autoComplete("from|", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "from");
            assert.deepEqual(wordlist, emptyList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 0);

            done();
        });
    });
    
    it('from should get collection names', done => {
        rqlTestUtils.autoComplete("from |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, collectionsList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });
    
    it('keyword with new lines', done => {
        rqlTestUtils.autoComplete(`from


|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, collectionsList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });
    
    it('from collection with new lines', done => {
        rqlTestUtils.autoComplete(`from
   
      
         
           
           
            
             
             Orders|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "Orders");
            assert.deepEqual(wordlist, collectionsList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 1);
            
            done();
        });
    });

    it('from Orders| should not list anything but have the Orders prefix', done => {
        rqlTestUtils.autoComplete("from Orders|", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "Orders");
            assert.deepEqual(wordlist, collectionsList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('from Collection | should list collections', done => {
        rqlTestUtils.autoComplete("from Orders |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            const sortedList = _.sortBy(wordlist, [(x: autoCompleteWordList) => x.score]).reverse();
            assert.deepEqual(sortedList, afterFromList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('from Collection select | should list fields', done => {
        rqlTestUtils.autoComplete("from Orders select |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "select");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('from Collection select nested field | should list nested fields with in prefix', done => {
        rqlTestUtils.autoComplete(`from Orders 
select ShipTo.in|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "in");
            assert.deepEqual(wordlist, fieldsShipToList);

            assert.equal(lastKeyword.keyword, "select");
            assert.equal(lastKeyword.dividersCount, 1);
            assert.deepEqual(lastKeyword.fieldPrefix, ["ShipTo"]);

            done();
        });
    });

    it('from Collection select nested field | without sapce should list fields with the City prefix', done => {
        rqlTestUtils.autoComplete(`from Orders 
select ShipTo.City|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "City");
            assert.deepEqual(wordlist, fieldsShipToList);

            assert.equal(lastKeyword.keyword, "select");
            assert.equal(lastKeyword.dividersCount, 1);
            assert.deepEqual(lastKeyword.fieldPrefix, ["ShipTo"]);

            done();
        });
    });

    it('from Collection select nested field | after should list as keyword only', done => {
        rqlTestUtils.autoComplete(`from Orders 
select ShipTo.City |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, [
                    {caption: "as", value: "as ", score: 3, meta: "keyword"},
                ]);

            assert.equal(lastKeyword.keyword, "select");
            assert.equal(lastKeyword.dividersCount, 2);
            assert.deepEqual(lastKeyword.fieldPrefix, ["ShipTo"]);

            done();
        });
    });

    it('from Collection select nested field | after comma should show more fields', done => {
        rqlTestUtils.autoComplete(`from Orders 
select ShipTo.City, |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "select");
            assert.equal(lastKeyword.dividersCount, 1);
            assert.isUndefined(lastKeyword.fieldPrefix);

            done();
        });
    });

    it('from Collection select nested field | right after comma should show more fields', done => {
        rqlTestUtils.autoComplete(`from Orders 
select ShipTo.City,|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "select");
            assert.equal(lastKeyword.dividersCount, 1);
            assert.isUndefined(lastKeyword.fieldPrefix);

            done();
        });
    });

    it('from AllDocs select nested field | after should list as keyword only', done => {
        rqlTestUtils.autoComplete(`from @all_docs
select |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, [
                {caption: "Max", value: "Max", score: 1, meta: "number field"},
                {caption: "@metadata", value: "@metadata", score: 1, meta: "object field"},
                {caption: "Name", value: "Name", score: 1, meta: "string field"},
                {caption: "Description", value: "Description", score: 1, meta: "string field"},
                {caption: "ExternalId", value: "ExternalId", score: 1, meta: "string field"},
                {caption: "Contact", value: "Contact", score: 1, meta: "object field"},
                {caption: "Address", value: "Address", score: 1, meta: "object field"},
                {caption: "Phone", value: "Phone", score: 1, meta: "string field"},
                {caption: "Fax", value: "Fax", score: 1, meta: "string field"},
                {caption: "LastName", value: "LastName", score: 1, meta: "string field"},
                {caption: "FirstName", value: "FirstName", score: 1, meta: "string field"},
                {caption: "Title", value: "Title", score: 1, meta: "string field"},
                {caption: "HiredAt", value: "HiredAt", score: 1, meta: "string field"},
                {caption: "Birthday", value: "Birthday", score: 1, meta: "string field"},
                {caption: "HomePhone", value: "HomePhone", score: 1, meta: "string field"},
                {caption: "Extension", value: "Extension", score: 1, meta: "string field"},
                {caption: "ReportsTo", value: "ReportsTo", score: 1, meta: "string field"},
                {caption: "Notes", value: "Notes", score: 1, meta: "null field"},
                {caption: "Territories", value: "Territories", score: 1, meta: "object[] | string[] field"},
                {caption: "Company", value: "Company", score: 1, meta: "string field"},
                {caption: "Employee", value: "Employee", score: 1, meta: "string field"},
                {caption: "OrderedAt", value: "OrderedAt", score: 1, meta: "string field"},
                {caption: "RequireAt", value: "RequireAt", score: 1, meta: "string field"},
                {caption: "ShippedAt", value: "ShippedAt", score: 1, meta: "null field"},
                {caption: "ShipTo", value: "ShipTo", score: 1, meta: "object field"},
                {caption: "ShipVia", value: "ShipVia", score: 1, meta: "string field"},
                {caption: "Freight", value: "Freight", score: 1, meta: "number field"},
                {caption: "Lines", value: "Lines", score: 1, meta: "object[] field"},
                {caption: "Na.me", value: "'Na.me'", score: 1, meta: "string field"},
                {caption: "Supplier", value: "Supplier", score: 1, meta: "string field"},
                {caption: "Category", value: "Category", score: 1, meta: "string field"},
                {caption: "QuantityPerUnit", value: "QuantityPerUnit", score: 1, meta: "string field"},
                {caption: "PricePerUnit", value: "PricePerUnit", score: 1, meta: "number field"},
                {caption: "UnitsInStock", value: "UnitsInStock", score: 1, meta: "number field"},
                {caption: "UnitsOnOrder", value: "UnitsOnOrder", score: 1, meta: "number field"},
                {caption: "Discontinued", value: "Discontinued", score: 1, meta: "boolean field"},
                {caption: "ReorderLevel", value: "ReorderLevel", score: 1, meta: "number field"},
                {caption: "HomePage", value: "HomePage", score: 1, meta: "null field"}
            ]);

            assert.equal(lastKeyword.keyword, "select");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('from a should get @all_docs', done => {
        rqlTestUtils.autoComplete("from a|", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "a");
            assert.deepEqual(wordlist, collectionsList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('from @ should get @all_docs', done => {
        rqlTestUtils.autoComplete("from @|", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "@");
            assert.deepEqual(wordlist, collectionsList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('from Collection where | should list fields', done => {
        rqlTestUtils.autoComplete("from Orders where |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('from Collection w | should has w prefix and select the where keyword', done => {
        rqlTestUtils.autoComplete(`from Orders 
w|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "w");
            const sortedList = _.sortBy(wordlist, [(x: autoCompleteWordList) => x.score]).reverse();
            assert.deepEqual(sortedList, afterFromList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('from Collection w| without space should has w prefix and select the where keyword', done => {
        rqlTestUtils.autoComplete(`from Orders
w|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "w");
            const sortedList = _.sortBy(wordlist, [(x: autoCompleteWordList) => x.score]).reverse();
            assert.deepEqual(sortedList, afterFromList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After where| without space should has where prefix should complete itself', done => {
        rqlTestUtils.autoComplete(`from Orders
where|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "where");
            const sortedList = _.sortBy(wordlist, [(x: autoCompleteWordList) => x.score]).reverse();
            assert.deepEqual(sortedList, afterWhereWithoutSpaceList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 0);

            done();
        });
    });

    it('After where we should list binary operation and other keywords', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            const sortedList = _.sortBy(wordlist, [(x: autoCompleteWordList) => x.score]).reverse();
            assert.deepEqual(sortedList, afterWhereList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 4);
            assert.equal(lastKeyword.operator, "=");

            done();
        });
    });

    it('WHERE and than AND without space should have AND prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' and|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "and");
            const sortedList = _.sortBy(wordlist, [(x: autoCompleteWordList) => x.score]).reverse();
            assert.deepEqual(sortedList, afterWhereList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 0);
            assert.equal(lastKeyword.binaryOperation, "and");
            assert.isUndefined(lastKeyword.operator);
            assert.isUndefined(lastKeyword.keywordModifier);

            done();
        });
    });

    it('WHERE and than AND should list fields', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' and |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });
    
    it('from index should get index names', done => {
        rqlTestUtils.autoComplete("from index |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, indexesList);

            assert.equal(lastKeyword.keyword, "from index");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('from index inside index name should complete with prefix', done => {
        rqlTestUtils.autoComplete("from index 'Orders/Tot|", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "'Orders/Tot");
            assert.deepEqual(wordlist, indexesList);

            assert.equal(lastKeyword.keyword, "from index");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('from index after index name without space should complete with prefix', done => {
        rqlTestUtils.autoComplete("from index 'Orders/Totals'|", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "'Orders/Totals'");
            assert.deepEqual(wordlist, indexesList);

            assert.equal(lastKeyword.keyword, "from index");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('from index after index name should complete with after from', done => {
        rqlTestUtils.autoComplete("from index 'Orders/Totals' |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            const sortedList = _.sortBy(wordlist, [(x: autoCompleteWordList) => x.score]).reverse();
            assert.deepEqual(sortedList, afterFromIndexList);

            assert.equal(lastKeyword.keyword, "from index");
            assert.equal(lastKeyword.dividersCount, 2);
            done();
        });
    });

    it('from index as', done => {
        rqlTestUtils.autoComplete("from index 'Orders/Totals' as |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(errors, ["empty completion"]);
            assert.isNull(wordlist);

            assert.equal(lastKeyword.keyword, "from index");
            assert.equal(lastKeyword.keywordModifier, "as");
            assert.equal(lastKeyword.dividersCount, 3);
            
            done();
        });
    });

    it('show fields of index', done => {
        rqlTestUtils.autoComplete("from index 'Orders/Totals' select |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, [
                {caption: "Employee", value: "Employee", score: 1, meta: "field"},
                {caption: "Company", value: "Company", score: 1, meta: "field"},
                {caption: "Total", value: "Total", score: 1, meta: "field"}
            ]);

            assert.equal(lastKeyword.keyword, "select");
            assert.equal(lastKeyword.dividersCount, 1);
            assert.isUndefined(lastKeyword.fieldPrefix);

            done();
        });
    });

    it('declare should suggest function', done => {
        rqlTestUtils.autoComplete("declare |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, [
                {caption: "function", value: "function ", score: 0, meta: "keyword"},
            ]);

            assert.equal(lastKeyword.keyword, "declare");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('decalre function should list empty list', done => {
        rqlTestUtils.autoComplete(`declare function CustomFunctionName(){}

|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, emptyList);

            assert.equal(lastKeyword.keyword, "declare function");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('decalre function without name should list empty list', done => {
        rqlTestUtils.autoComplete(`declare function(){}

|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, emptyList);

            assert.equal(lastKeyword.keyword, "declare function");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });
});

