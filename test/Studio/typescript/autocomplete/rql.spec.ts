/// <reference path="../../../../src/Raven.Studio/typings/tsd.d.ts" />

import chai = require("chai");
const assert = chai.assert;

import rqlTestUtils = require("autocomplete/rqlTestUtils");

const emptyProvider = rqlTestUtils.emptyProvider;
const northwindProvider = rqlTestUtils.northwindProvider;

describe("RQL Autocomplete", () => {

    const emptyList: autoCompleteWordList[] = [
        {caption: "from", value: "from ", score: 2, meta: "keyword"},
        {caption: "declare", value: "declare ", score: 1, meta: "keyword", snippet: `declare function \${1:Name}() {
    \${0}
}

`}
    ];

    const collectionsList = [
        {caption: "index", value: "index ", score: 4, meta: "keyword"},
        {caption: "@all_docs", value: "@all_docs ", score: 3, meta: "collection"},
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
        {caption: "Collection With ' And \" in name", value: "'Collection With '' And \" in name' ", score: 2, meta: "collection"}
    ];

    const indexesList = [
        {caption: "Orders/ByCompany", value: "'Orders/ByCompany' ", score: 101, meta: "index"},
        {caption: "Product/Sales", value: "'Product/Sales' ", score: 101, meta: "index"},
        {caption: "Orders/Totals", value: "'Orders/Totals' ", score: 101, meta: "index"},
        {caption: "Index With ' And \" in name", value: "'Index With '' And \" in name' ", score: 101, meta: "index"},
    ];

    const functionsList = [
        {caption: "ID", value: "ID() ", score: 11, meta: "document ID"}
    ];

    const whereFunctionsList: autoCompleteWordList[] = [
        {caption: "search", value: "search ", snippet: "search(${1:alias.Field.Name}, ${2:'*term1* term2*'}, ${3:or}) ", score: 21, meta: "function"}
    ];

    const fieldsList: autoCompleteWordList[] = [
        {caption: "Company", value: "Company ", score: 114, meta: "string field"},
        {caption: "Employee", value: "Employee ", score: 113, meta: "string field"},
        {caption: "Freight", value: "Freight ", score: 112, meta: "number field"},
        {caption: "Lines", value: "Lines ", score: 111, meta: "object[] field"},
        {caption: "OrderedAt", value: "OrderedAt ", score: 110, meta: "string field"},
        {caption: "RequireAt", value: "RequireAt ", score: 109, meta: "string field"},
        {caption: "ShipTo", value: "ShipTo ", score: 108, meta: "object field"},
        {caption: "ShipVia", value: "ShipVia ", score: 107, meta: "string field"},
        {caption: "ShippedAt", value: "ShippedAt ", score: 106, meta: "string field"},
        {caption: "With ' and \" quotes", value: "'With '' and \" quotes' ", score: 105, meta: "string field"},
        {caption: "With Space", value: "'With Space' ", score: 104, meta: "string field"},
        {caption: "With*Star", value: "'With*Star' ", score: 103, meta: "string field"},
        {caption: "With.Dot", value: "'With.Dot' ", score: 102, meta: "string field"},
        {caption: "@metadata", value: "@metadata ", score: 101, meta: "object field"}
    ].concat(functionsList);

    const whereFieldsList = _.sortBy(fieldsList.concat(whereFunctionsList), (x: autoCompleteWordList) => x.score).reverse();

    const allDocsFieldsList = [
        {caption: "Address", value: "Address ", score: 138, meta: "object field"},
        {caption: "Birthday", value: "Birthday ", score: 137, meta: "string field"},
        {caption: "Category", value: "Category ", score: 136, meta: "string field"},
        {caption: "Company", value: "Company ", score: 135, meta: "string field"},
        {caption: "Contact", value: "Contact ", score: 134, meta: "object field"},
        {caption: "Description", value: "Description ", score: 133, meta: "string field"},
        {caption: "Discontinued", value: "Discontinued ", score: 132, meta: "boolean field"},
        {caption: "Employee", value: "Employee ", score: 131, meta: "string field"},
        {caption: "Extension", value: "Extension ", score: 130, meta: "string field"},
        {caption: "ExternalId", value: "ExternalId ", score: 129, meta: "string field"},
        {caption: "Fax", value: "Fax ", score: 128, meta: "string field"},
        {caption: "FirstName", value: "FirstName ", score: 127, meta: "string field"},
        {caption: "Freight", value: "Freight ", score: 126, meta: "number field"},
        {caption: "HiredAt", value: "HiredAt ", score: 125, meta: "string field"},
        {caption: "HomePage", value: "HomePage ", score: 124, meta: "null field"},
        {caption: "HomePhone", value: "HomePhone ", score: 123, meta: "string field"},
        {caption: "LastName", value: "LastName ", score: 122, meta: "string field"},
        {caption: "Lines", value: "Lines ", score: 121, meta: "object[] field"},
        {caption: "Max", value: "Max ", score: 120, meta: "number field"},
        {caption: "Na.me", value: "'Na.me' ", score: 119, meta: "string field"},
        {caption: "Name", value: "Name ", score: 118, meta: "string field"},
        {caption: "Notes", value: "Notes ", score: 117, meta: "null field"},
        {caption: "OrderedAt", value: "OrderedAt ", score: 116, meta: "string field"},
        {caption: "Phone", value: "Phone ", score: 115, meta: "string field"},
        {caption: "PricePerUnit", value: "PricePerUnit ", score: 114, meta: "number field"},
        {caption: "QuantityPerUnit", value: "QuantityPerUnit ", score: 113, meta: "string field"},
        {caption: "ReorderLevel", value: "ReorderLevel ", score: 112, meta: "number field"},
        {caption: "ReportsTo", value: "ReportsTo ", score: 111, meta: "string field"},
        {caption: "RequireAt", value: "RequireAt ", score: 110, meta: "string field"},
        {caption: "ShipTo", value: "ShipTo ", score: 109, meta: "object field"},
        {caption: "ShipVia", value: "ShipVia ", score: 108, meta: "string field"},
        {caption: "ShippedAt", value: "ShippedAt ", score: 107, meta: "null field"},
        {caption: "Supplier", value: "Supplier ", score: 106, meta: "string field"},
        {caption: "Territories", value: "Territories ", score: 105, meta: "object[] | string[] field"},
        {caption: "Title", value: "Title ", score: 104, meta: "string field"},
        {caption: "UnitsInStock", value: "UnitsInStock ", score: 103, meta: "number field"},
        {caption: "UnitsOnOrder", value: "UnitsOnOrder ", score: 102, meta: "number field"},
        {caption: "@metadata", value: "@metadata ", score: 101, meta: "object field"}
    ].concat(functionsList);
    
    const orderByFieldsList = _.sortBy(fieldsList.concat([
        {caption: "score", value: "score() ", snippet: "score() ", score: 22, meta: "function"}, // TODO: snippet
        {caption: "random", value: "random() ", snippet: "random() ", score: 21, meta: "function"} // TODO: snippet
    ]), (x: autoCompleteWordList) => x.score).reverse(); 
    
    const orderBySortAfterList = [
        {caption: ",", value: ", ", score: 23, meta: "separator"},
        {caption: "load", value: "load ", score: 20, meta: "keyword"},
        {caption: "select", value: "select ", score: 19, meta: "keyword"},
        {caption: "include", value: "include ", score: 18, meta: "keyword"}
    ];
    
    const groupByAfterList = [
        {caption: ",", value: ", ", score: 23, meta: "separator"},
        {caption: "where", value: "where ", score: 20, meta: "keyword"},
        {caption: "order", value: "order ", score: 19, meta: "keyword"},
        {caption: "load", value: "load ", score: 18, meta: "keyword"},
        {caption: "select", value: "select ", score: 17, meta: "keyword"},
        {caption: "include", value: "include ", score: 16, meta: "keyword"}
    ];
    
    const orderBySortList =  _.sortBy(orderBySortAfterList.concat([
        {caption: "desc", value: "desc ", score: 22, meta: "descending sort"},
        {caption: "asc", value: "asc ", score: 21, meta: "ascending sort"}
    ]), (x: autoCompleteWordList) => x.score).reverse();

    const fieldsShipToList = [
        {caption: "City", value: "City ", score: 106, meta: "string field"},
        {caption: "Country", value: "Country ", score: 105, meta: "string field"},
        {caption: "Line1", value: "Line1 ", score: 104, meta: "string field"},
        {caption: "Line2", value: "Line2 ", score: 103, meta: "null field"},
        {caption: "PostalCode", value: "PostalCode ", score: 102, meta: "string field"},
        {caption: "Region", value: "Region ", score: 101, meta: "string field"}
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

    const afterGroupWithoutSpaceList = [
        {caption: "group", value: "group ", score: 20, meta: "keyword"},
        {caption: "where", value: "where ", score: 19, meta: "keyword"},
        {caption: "order", value: "order ", score: 18, meta: "keyword"},
        {caption: "load", value: "load ", score: 17, meta: "keyword"},
        {caption: "select", value: "select ", score: 16, meta: "keyword"},
        {caption: "include", value: "include ", score: 15, meta: "keyword"}
    ];

    const afterIncludeWithoutSpaceList = [
        {caption: "include", value: "include ", score: 20, meta: "keyword"}
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

    const afterOrderWithoutSpaceList = [
        {caption: "order", value: "order ", score: 20, meta: "keyword"},
        {caption: "load", value: "load ", score: 19, meta: "keyword"},
        {caption: "select", value: "select ", score: 18, meta: "keyword"},
        {caption: "include", value: "include ", score: 17, meta: "keyword"}
    ];

    const afterOrderOrGroupList = [
        {caption: "by", value: "by ", score: 21, meta: "keyword"}
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
            assert.deepEqual(wordlist, afterFromList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('from collection with a new line', done => {
        rqlTestUtils.autoComplete(`from Orders 
|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, afterFromList);

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
            assert.deepEqual(wordlist, allDocsFieldsList);

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

    it('from Collection w | should has w prefix and select the where keyword', done => {
        rqlTestUtils.autoComplete(`from Orders 
w|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "w");
            assert.deepEqual(wordlist, afterFromList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('from Collection w| without space should has w prefix and select the where keyword', done => {
        rqlTestUtils.autoComplete(`from Orders
w|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "w");
            assert.deepEqual(wordlist, afterFromList);

            assert.equal(lastKeyword.keyword, "from");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After where| without space should has where prefix and should complete itself', done => {
        rqlTestUtils.autoComplete(`from Orders
where|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "where");
            assert.deepEqual(wordlist, afterWhereWithoutSpaceList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 0);

            done();
        });
    });

    it('from Collection where | should list fields', done => {
        rqlTestUtils.autoComplete("from Orders where |", northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, whereFieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After where field without space | should list itself with prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "OrderedAt");
            assert.deepEqual(wordlist, whereFieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it.skip('After where field | should list binary operators', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, [/*binary operators*/]);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and equal operator without space | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt =|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and equal operator | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt = |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and in operator | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt in |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and in operator | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt in (|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and all in operator | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt all in |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and all in operator | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt all in (|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and between operator | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt between |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and between operator 2 | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt between (1) |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and between operator 3 | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt between (1) and |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it.skip('After where field and between operator 4 | ?????????????????????????????', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt between (1) and (2) |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After where function without open parentheses | should list itself', done => {
        rqlTestUtils.autoComplete(`from Orders
where search|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "search");
            assert.deepEqual(wordlist, whereFieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it.skip('After where function first parameter | should list fields without where functions', done => {
        rqlTestUtils.autoComplete(`from Orders
where search(|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After where we should list binary operation and other keywords', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, afterWhereList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 4);
            //TODO: assert.equal(lastKeyword.operator, "=");

            done();
        });
    });

    it('WHERE and than AND without space should have AND prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' and|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "and");
            assert.deepEqual(wordlist, afterWhereList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 0);
            assert.equal(lastKeyword.binaryOperation, "and");
            //TODO: assert.isUndefined(lastKeyword.operator);
            assert.isUndefined(lastKeyword.keywordModifier);

            done();
        });
    });

    it('WHERE and than AND should list fields', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' and |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, whereFieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After order| without space should has order prefix and should complete itself', done => {
        rqlTestUtils.autoComplete(`from Orders
order|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "order");
            assert.deepEqual(wordlist, afterOrderWithoutSpaceList);

            assert.equal(lastKeyword.keyword, "order");
            assert.equal(lastKeyword.dividersCount, 0);

            done();
        });
    });

    it('After order | should list BY', done => {
        rqlTestUtils.autoComplete(`from Orders
order |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, afterOrderOrGroupList);

            assert.equal(lastKeyword.keyword, "order");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After order by| without space should has BY prefix and should complete itself', done => {
        rqlTestUtils.autoComplete(`from Orders
order by|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "by");
            assert.deepEqual(wordlist, afterOrderOrGroupList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 0);

            done();
        });
    });

    it('After order by | should list fields and functions', done => {
        rqlTestUtils.autoComplete(`from Orders
order by |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, orderByFieldsList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After order by field| without space should list the fields with prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
order by OrderedAt|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "OrderedAt");
            assert.deepEqual(wordlist, orderByFieldsList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it.skip('After order by field with dot | without space should list the fields without the functions', done => {
        rqlTestUtils.autoComplete(`from Orders
order by OrderedAt.|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, orderByFieldsList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After order by field | should list the sort functions and comma separator', done => {
        rqlTestUtils.autoComplete(`from Orders
order by OrderedAt |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, orderBySortList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After order by field and desc| without space should list desc prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
order by OrderedAt desc|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "desc");
            assert.deepEqual(wordlist, orderBySortList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After order by field with sort | should list the comma separator without the sort functions', done => {
        rqlTestUtils.autoComplete(`from Orders
order by OrderedAt desc |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, orderBySortAfterList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 3);

            done();
        });
    });

    it('After order by field and comma | should list the fields', done => {
        rqlTestUtils.autoComplete(`from Orders
order by OrderedAt, |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, orderByFieldsList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After order by field and comma without space | should list the fields', done => {
        rqlTestUtils.autoComplete(`from Orders
order by OrderedAt,|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, orderByFieldsList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After order by field, sort and comma without space | should list the fields', done => {
        rqlTestUtils.autoComplete(`from Orders
order by OrderedAt desc, |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, orderByFieldsList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After order by field, sort and comma without space | should list the fields', done => {
        rqlTestUtils.autoComplete(`from Orders
order by OrderedAt desc,|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, orderByFieldsList);

            assert.equal(lastKeyword.keyword, "order by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After include| without space should complete itself with prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
include|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "include");
            assert.deepEqual(wordlist, afterIncludeWithoutSpaceList);

            assert.equal(lastKeyword.keyword, "include");
            assert.equal(lastKeyword.dividersCount, 0);

            done();
        });
    });

    it('After include | should list fields', done => {
        rqlTestUtils.autoComplete(`from Orders
include |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "include");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After include and field without space | should list fields with prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
include Employee|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "Employee");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "include");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After include and field | should not list anything', done => {
        rqlTestUtils.autoComplete(`from Orders
include Employee |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.isNull(wordlist);

            assert.equal(lastKeyword.keyword, "include");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After group| without space should has group prefix and should complete itself', done => {
        rqlTestUtils.autoComplete(`from Orders
group|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "group");
            assert.deepEqual(wordlist, afterGroupWithoutSpaceList);

            assert.equal(lastKeyword.keyword, "group");
            assert.equal(lastKeyword.dividersCount, 0);

            done();
        });
    });

    it('After group | should list BY', done => {
        rqlTestUtils.autoComplete(`from Orders
group |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, afterOrderOrGroupList);

            assert.equal(lastKeyword.keyword, "group");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After group by| without space should has BY prefix and should complete itself', done => {
        rqlTestUtils.autoComplete(`from Orders
group by|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "by");
            assert.deepEqual(wordlist, afterOrderOrGroupList);

            assert.equal(lastKeyword.keyword, "group by");
            assert.equal(lastKeyword.dividersCount, 0);

            done();
        });
    });

    it('After group by | should list fields', done => {
        rqlTestUtils.autoComplete(`from Orders
group by |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "group by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After group by without space | should list itself with prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
group by ShippedAt|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "ShippedAt");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "group by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After group by | should list comma or next keywords', done => {
        rqlTestUtils.autoComplete(`from Orders
group by ShippedAt |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, groupByAfterList);

            assert.equal(lastKeyword.keyword, "group by");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After group by comma without space | should fields', done => {
        rqlTestUtils.autoComplete(`from Orders
group by ShippedAt,|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "group by");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After group by comma | should fields', done => {
        rqlTestUtils.autoComplete(`from Orders
group by ShippedAt, |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "group by");
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
            assert.deepEqual(wordlist, afterFromIndexList);

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
                {caption: "Employee", value: "Employee ", score: 101, meta: "field"},
                {caption: "Company", value: "Company ", score: 101, meta: "field"},
                {caption: "Total", value: "Total ", score: 101, meta: "field"}
            ].concat(functionsList));

            assert.equal(lastKeyword.keyword, "select");
            assert.equal(lastKeyword.dividersCount, 1);
            assert.isUndefined(lastKeyword.fieldPrefix);

            done();
        });
    });

    it('dec| should list the declare function with prefix', done => {
        rqlTestUtils.autoComplete(`dec|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "dec");
            assert.deepEqual(wordlist, emptyList);

            assert.isNull(lastKeyword);

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

            assert.isUndefined(lastKeyword.keyword);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('decalre function without name should list empty list', done => {
        rqlTestUtils.autoComplete(`declare function(){}

|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, emptyList);

            assert.isUndefined(lastKeyword.keyword);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('decalre function with 3 new lines should list empty list', done => {
        rqlTestUtils.autoComplete(`declare function Name() {
    
}


|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, emptyList);

            assert.isUndefined(lastKeyword.keyword);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });
});

