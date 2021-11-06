
/*
static northwindProvider() {
    const providers: queryCompleterProviders = {
        t
        collections: callback => {
            callback([
                "Regions",
                "Suppliers",
                "Employees",
                "Categories",
                "Products",
                "Shippers",
                "Companies",
                "Orders",
                "Collection With Space",
                "Collection!",
                "Collection With ' And \" in name"
            ]);
        },
        indexFields: (indexName, callback) => {
            switch (indexName){
                case "Orders/Totals":
                    callback([
                        "Employee",
                        "Company",
                        "Total"
                    ]);
                    break;
                default:
                    callback([]);
                    break;
            }
        },
        collectionFields: (collectionName, prefix, callback) => {
            switch (collectionName){
                case "Orders":

                    switch (prefix) {
                        case "ShipTo":
                            callback({
                                "Line1": "String",
                                "Line2": "Null",
                                "City": "String",
                                "Region": "String",
                                "PostalCode": "String",
                                "Country": "String",
                                "Location": "Object",
                            });
                            break;
                        case "ShipTo.Location":
                            callback({
                                "Latitude": "Number",
                                "Longitude": "Number",
                                "L1": "String",
                                "L2": "Null",
                                "P": "String",
                                "NestedObject": "Object",
                            });
                            break;
                        case "ShipTo.Location.NestedObject":
                            callback({
                                "C2": "String",
                                "R2": "String",
                                "P2": "String"
                            });
                            break;
                        case "Lines":
                            callback({
                                "Discount": "Number",
                                "PricePerUnit": "Number",
                                "Product": "String",
                                "ProductName": "String",
                                "Quantity": "Number"
                            });
                            break;
                        default:
                            callback({
                                "Company": "String",
                                "Employee": "String",
                                "OrderedAt": "String",
                                "RequireAt": "String",
                                "ShippedAt": "String",
                                "ShipTo": "Object",
                                "ShipVia": "String",
                                "Freight": "Number",
                                "Lines": "ArrayObject",
                                "With.Dot": "String",
                                "With*Star": "String",
                                "With Space": "String",
                                "With ' and \" quotes": "String",
                                "@metadata": "Object"
                            });
                            break;
                    }

                    break;
                case "@all_docs":

                    switch (prefix) {
                        default:
                            callback({
                                "Max": "Number",
                                "@metadata": "Object",
                                "Name": "String",
                                "Description": "String",
                                "ExternalId": "String",
                                "Contact": "Object",
                                "Address": "Object",
                                "Phone": "String",
                                "Fax": "String",
                                "LastName": "String",
                                "FirstName": "String",
                                "Title": "String",
                                "HiredAt": "String",
                                "Birthday": "String",
                                "HomePhone": "String",
                                "Extension": "String",
                                "ReportsTo": "String",
                                "Notes": "Null",
                                "Territories": "ArrayObject, ArrayString",
                                "Company": "String",
                                "Employee": "String",
                                "OrderedAt": "String",
                                "RequireAt": "String",
                                "ShippedAt": "Null",
                                "ShipTo": "Object",
                                "ShipVia": "String",
                                "Freight": "Number",
                                "Lines": "ArrayObject",
                                "Na.me": "String",
                                "Supplier": "String",
                                "Category": "String",
                                "QuantityPerUnit": "String",
                                "PricePerUnit": "Number",
                                "UnitsInStock": "Number",
                                "UnitsOnOrder": "Number",
                                "Discontinued": "Boolean",
                                "ReorderLevel": "Number",
                                "HomePage": "Null"
                            });
                            break;
                    }

                    break;
                default:
                    callback({});
                    break;
            }

        },
        indexNames: callback => callback([
            "Orders/ByCompany",
            "Product/Sales",
            "Orders/Totals",
            "Index With ' And \" in name"
        ])
    };
    const completer = new queryCompleter(providers, "Select");

    return () => completer;
}


//TODO: migrate

describe("RQL Autocomplete", () => {


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

    const aliasList: autoCompleteWordList[] = [
        {caption: "o", value: "o.", score: 999, meta: "Orders"}
    ];

    const aliasListAfterWhere: autoCompleteWordList[] =  _.sortBy(aliasList.concat(functionsList).concat(queryCompleter.whereFunctionsOnly), (x: autoCompleteWordList) => x.score).reverse();

    const aliasesList: autoCompleteWordList[] = aliasList.concat([
        {caption: "c", value: "c.", score: 998, meta: "Company"},
        {caption: "e", value: "e.", score: 997, meta: "Employee"},
        {caption: "s", value: "s.", score: 996, meta: "ShipVia"}
    ]);

    const aliasesListWithFunctions: autoCompleteWordList[] =  _.sortBy(aliasesList.concat(queryCompleter.functionsList), (x: autoCompleteWordList) => x.score).reverse();

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
    ];

    const fieldsListWithFunctions: autoCompleteWordList[] = fieldsList.concat(functionsList);

    const whereFieldsList: autoCompleteWordList[] = _.sortBy(fieldsListWithFunctions.concat(queryCompleter.whereFunctionsOnly), (x: autoCompleteWordList) => x.score).reverse();

    const whereFieldsListAfterOrAnd = queryCompleter.notList.concat(whereFieldsList);

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
        {caption: "load", value: "load ", score: 20, meta: "clause", snippet: "load ${1:field} as ${2:alias} "},
        {caption: "select", value: "select ", score: 19, meta: "keyword"},
        {caption: "select {", value: "select { ", score: 18, meta: "JS projection", snippet: `select {
    \${1:Name}: \${2:Value}
}
`},
        {caption: "include", value: "include ", score: 17, meta: "keyword"}
    ];

    const groupByAfterList = [
        {caption: ",", value: ", ", score: 23, meta: "separator"},
        {caption: "where", value: "where ", score: 20, meta: "keyword"},
        {caption: "order", value: "order ", score: 19, meta: "keyword"},
        {caption: "load", value: "load ", score: 18, meta: "clause", snippet: "load ${1:field} as ${2:alias} "},
        {caption: "select", value: "select ", score: 17, meta: "keyword"},
        {caption: "select {", value: "select { ", score: 16, meta: "JS projection", snippet: `select {
    \${1:Name}: \${2:Value}
}
`},
        {caption: "include", value: "include ", score: 15, meta: "keyword"}
    ];

    const orderBySortList =  _.sortBy(orderBySortAfterList.concat([
        {caption: "desc", value: "desc ", score: 22, meta: "descending sort"},
        {caption: "asc", value: "asc ", score: 21, meta: "ascending sort"}
    ]), (x: autoCompleteWordList) => x.score).reverse();

    const fieldsShipToList = [
        {caption: "City", value: "City ", score: 107, meta: "string field"},
        {caption: "Country", value: "Country ", score: 106, meta: "string field"},
        {caption: "Line1", value: "Line1 ", score: 105, meta: "string field"},
        {caption: "Line2", value: "Line2 ", score: 104, meta: "null field"},
        {caption: "Location", value: "Location ", score: 103, meta: "object field"},
        {caption: "PostalCode", value: "PostalCode ", score: 102, meta: "string field"},
        {caption: "Region", value: "Region ", score: 101, meta: "string field"}
    ];

    const afterFromList = [
        {caption: "as", value: "as ", score: 21, meta: "keyword"},
        {caption: "group", value: "group ", score: 20, meta: "keyword"},
        {caption: "where", value: "where ", score: 19, meta: "keyword"},
        {caption: "order", value: "order ", score: 18, meta: "keyword"},
        {caption: "load", value: "load ", score: 17, meta: "clause", snippet: "load ${1:field} as ${2:alias} "},
        {caption: "select", value: "select ", score: 16, meta: "keyword"},
        {caption: "select {", value: "select { ", score: 15, meta: "JS projection", snippet: `select {
    \${1:Name}: \${2:Value}
}
`},
        {caption: "include", value: "include ", score: 14, meta: "keyword"}
    ];

    const afterFromAsList = [
        {caption: "group", value: "group ", score: 20, meta: "keyword"},
        {caption: "where", value: "where ", score: 19, meta: "keyword"},
        {caption: "order", value: "order ", score: 18, meta: "keyword"},
        {caption: "load", value: "load ", score: 17, meta: "clause", snippet: "load o.${1:field} as ${2:alias} "},
        {caption: "select", value: "select ", score: 16, meta: "keyword"},
        {caption: "select {", value: "select { ", score: 15, meta: "JS projection", snippet: `select {
    \${1:Name}: \${2:Value}
}
`},
        {caption: "include", value: "include ", score: 14, meta: "keyword"}
    ];

    const afterFromIndexList = [
        {caption: "as", value: "as ", score: 21, meta: "keyword"},
        {caption: "where", value: "where ", score: 20, meta: "keyword"},
        {caption: "order", value: "order ", score: 19, meta: "keyword"},
        {caption: "load", value: "load ", score: 18, meta: "clause", snippet: "load ${1:field} as ${2:alias} "},
        {caption: "select", value: "select ", score: 17, meta: "keyword"},
        {caption: "select {", value: "select { ", score: 16, meta: "JS projection", snippet: `select {
    \${1:Name}: \${2:Value}
}
`},
        {caption: "include", value: "include ", score: 15, meta: "keyword"}
    ];



    const afterGroupWithoutSpaceList = [
        {caption: "group", value: "group ", score: 20, meta: "keyword"},
        {caption: "where", value: "where ", score: 19, meta: "keyword"},
        {caption: "order", value: "order ", score: 18, meta: "keyword"},
        {caption: "load", value: "load ", score: 17, meta: "clause", snippet: "load ${1:field} as ${2:alias} "},
        {caption: "select", value: "select ", score: 16, meta: "keyword"},
        {caption: "select {", value: "select { ", score: 15, meta: "JS projection", snippet: `select {
    \${1:Name}: \${2:Value}
}
`},
        {caption: "include", value: "include ", score: 14, meta: "keyword"}
    ];

    const afterIncludeWithoutSpaceList = [
        {caption: "include", value: "include ", score: 20, meta: "keyword"}
    ];



    it('from collection as alias, where | should list aliases with functions', done => {
        rqlTestUtils.autoComplete(`from Orders as o
where |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, aliasListAfterWhere);

            assert.equal(lastKeyword.keyword, "where");
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 1);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders"});

            done();
        });
    });

    it('from collection as alias, where and alias with dot | should list collection fields', done => {
        rqlTestUtils.autoComplete(`from Orders as o
where c.|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 1);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders"});

            done();
        });
    });

    it('from collection as alias, load | should list alias', done => {
        rqlTestUtils.autoComplete(`from Orders as o
load |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, aliasList);

            assert.equal(lastKeyword.keyword, "load");
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 1);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders"});

            done();
        });
    });

    it('from collection as alias, load and alias with dot | should list collection fields', done => {
        rqlTestUtils.autoComplete(`from Orders as o
load o.|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "load");
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 1);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders"});

            done();
        });
    });

    it('from collection as alias, load and alias with dot than space | should list as only', done => {
        rqlTestUtils.autoComplete(`from Orders as o
load o.Company |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, queryCompleter.asList);

            assert.equal(lastKeyword.keyword, "load");
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 2);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders"});

            done();
        });
    });

    it('from collection as alias, load and alias with space | should list nothing', done => {
        rqlTestUtils.autoComplete(`from Orders as o
load o.Company as |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.isNull(wordlist);

            assert.equal(lastKeyword.keyword, "load");
            assert.isTrue(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 3);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders"});

            done();
        });
    });

    it('from collection as alias, load and alias specified | should list separator and next keywords', done => {
        rqlTestUtils.autoComplete(`from Orders as o
load o.Company as c |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, afterLoad);

            assert.equal(lastKeyword.keyword, "load");
            assert.isTrue(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 4);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders", c: "Company"});

            done();
        });
    });

    it('from collection as alias, load and separator without space | should list separator and next keywords', done => {
        rqlTestUtils.autoComplete(`from Orders as o
load o.Company as c,|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, aliasesList.slice(0, 2));

            assert.equal(lastKeyword.keyword, "load");
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 1);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders", c: "Company"});

            done();
        });
    });

    it('from collection as alias, load and separator | should list separator and next keywords', done => {
        rqlTestUtils.autoComplete(`from Orders as o
load o.Company as c, |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, aliasesList.slice(0, 2));

            assert.equal(lastKeyword.keyword, "load");
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 1);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders", c: "Company"});

            done();
        });
    });

    it('from collection as alias, where | should list aliases with functions', done => {
        rqlTestUtils.autoComplete(`from Orders as o
load o.Company as c, o.Employee as e, o.ShipVia as s
select |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, aliasesListWithFunctions);

            assert.equal(lastKeyword.keyword, "select");
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 1);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.equal(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders", c: "Company", e: "Employee", s: "ShipVia"});

            done();
        });
    });


    it('from Collection select nested field | after comma should show more fields', done => {
        rqlTestUtils.autoComplete(`from Orders 
select ShipTo.City, |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsListWithFunctions);

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
            assert.deepEqual(wordlist, fieldsListWithFunctions);

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
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 1);
            assert.equal(lastKeyword.info.collection, "@all_docs");
            assert.isUndefined(lastKeyword.info.index);
            assert.isUndefined(lastKeyword.info.alias);
            assert.isUndefined(lastKeyword.info.aliases);

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

    it('After where field | should list binary operators', done => {
        rqlTestUtils.autoComplete(`from Orders
where Freight |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, queryCompleter.whereOperators);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After where field and equal operator without space | should list binray operators with prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt =|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "=");
            assert.deepEqual(wordlist, queryCompleter.whereOperators);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After where field and equal operator | list nothing because of empty prefix', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt = |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, termsOrderOrderedAt);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 3);

            done();
        });
    });

    it('After where field and equal operator with prefix | list document ID', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt = com|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "com");
            assert.deepEqual(wordlist, termsOrderOrderedAt);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 3);

            done();
        });
    });

    it('After where field and equal operator after prefix | list document ID', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt = 'companies/1-A'|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "'companies/1-A'");
            assert.deepEqual(wordlist, termsOrderOrderedAt);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 3);

            done();
        });
    });

    it('After where field and in operator | before ( should not list anything', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt in |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.isNull(wordlist);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 3);
            assert.equal(lastKeyword.whereFunction, "in");
            assert.equal(lastKeyword.whereFunctionParameters, 0);
            assert.equal(lastKeyword.parentheses, 0);

            done();
        });
    });

    it('After where field and in operator | after ( should list terms', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt in (|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, termsOrderOrderedAt);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 3);
            assert.equal(lastKeyword.whereFunction, "in");
            assert.equal(lastKeyword.whereFunctionParameters, 0);
            assert.equal(lastKeyword.parentheses, 1);

            done();
        });
    });

    it('After where field and in operator | after (, should list terms', done => {
        rqlTestUtils.autoComplete(`from Orders
where OrderedAt in ('1996-07-18T00:00:00.0000000', |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, termsOrderOrderedAt);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 3);
            assert.equal(lastKeyword.whereFunction, "in");
            assert.equal(lastKeyword.whereFunctionParameters, 0);
            assert.equal(lastKeyword.parentheses, 1);

            done();
        });
    });

    it('After where function without open parentheses | should list itself', done => {
        rqlTestUtils.autoComplete(`from Orders
where search|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "search");
            assert.deepEqual(wordlist, whereFieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.whereFunctionParameters, 0);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After where function first parameter | should list fields without where functions', done => {
        rqlTestUtils.autoComplete(`from Orders
where search(|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.whereFunctionParameters, 1);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After where function first parameter with prefix | should list itself with prefix.', done => {
        rqlTestUtils.autoComplete(`from Orders
where search(OrderedAt|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "OrderedAt");
            assert.deepEqual(wordlist, fieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.whereFunctionParameters, 1);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After where function second parameter without space | should list terms. TODO.', done => {
        rqlTestUtils.autoComplete(`from Orders
where search(OrderedAt,|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.isNull(wordlist);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.whereFunctionParameters, 2);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After where function second parameter | should list terms. TODO.', done => {
        rqlTestUtils.autoComplete(`from Orders
where search(OrderedAt, |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.isNull(wordlist);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.whereFunctionParameters, 2);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After where function third parameter | should list OR and AND.', done => {
        rqlTestUtils.autoComplete(`from Orders
where search(OrderedAt, '1996*', |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, searchOrAndList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.whereFunctionParameters, 3);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After where function third parameter with prefix | should list OR and AND with prefix.', done => {
        rqlTestUtils.autoComplete(`from Orders
where search(OrderedAt  ,   '1996*'  ,   and|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "and");
            assert.deepEqual(wordlist, searchOrAndList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.whereFunctionParameters, 3);
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('After where function without space | should list binary operation and next keywords', done => {
        rqlTestUtils.autoComplete(`from Orders
where search(OrderedAt, '1996*')|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, afterWhereList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After where function | should list binary operation and next keywords', done => {
        rqlTestUtils.autoComplete(`from Orders
where search(OrderedAt, '1996*') |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, afterWhereList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.dividersCount, 2);

            done();
        });
    });

    it('After where function | complex combination of where functions', done => {
        rqlTestUtils.autoComplete(`from Orders as o
where search(OrderedAt, "*1997*", or) or Freight = "" or (Freight = "" or Freight = "") 
and (Freight = "" and Freight = "") and search(OrderedAt, "*1997*", and)|`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, afterWhereListWithFromAs);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.binaryOperation, "and");
            assert.equal(lastKeyword.whereFunction, "search");
            assert.equal(lastKeyword.whereFunctionParameters, 3);
            assert.equal(lastKeyword.parentheses, 0);
            assert.isFalse(lastKeyword.asSpecified);
            assert.equal(lastKeyword.dividersCount, 2);
            assert.equal(lastKeyword.info.collection, "Orders");
            assert.isUndefined(lastKeyword.info.index);
            assert.deepEqual(lastKeyword.info.alias, "o");
            assert.deepEqual(lastKeyword.info.aliases, {o: "Orders"});

            done();
        });
    });

    it('After where | should list binary operation and next keywords', done => {
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
            assert.isFalse(lastKeyword.asSpecified);

            done();
        });
    });

    it('WHERE and than AND | should list fields and NOT keyword', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' and |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, whereFieldsListAfterOrAnd);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('WHERE and than OR | should list fields and NOT keyword', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' or |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, whereFieldsListAfterOrAnd);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('WHERE and than AND and NOT | should list fields with where functions', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' and not |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, whereFieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

    it('WHERE and than OR and NOT | should list fields with where functions', done => {
        rqlTestUtils.autoComplete(`from Orders
where ShipTo.Country = 'France' or not |`, northwindProvider(), (errors, wordlist, prefix, lastKeyword) => {
            assert.equal(prefix, "");
            assert.deepEqual(wordlist, whereFieldsList);

            assert.equal(lastKeyword.keyword, "where");
            assert.equal(lastKeyword.dividersCount, 1);

            done();
        });
    });

  
*/
