/// <reference path="../../../src/Raven.Studio/typings/tsd.d.ts" />

import chai = require("chai");
const assert = chai.assert;

import queryUtil = require("src/Raven.Studio/typescript/common/queryUtil");

describe("queryUtil", () => {


    describe('replaceSelectWithFetchAllStoredFields()', () => {

        it ('with out select', () => {
            const query = "from index 'Orders/ByCompany'";
            const expected = `from index 'Orders/ByCompany' select __all_stored_fields`;
            test(query, expected);
        });

        it ('with simple select', () => {
            const query = "from index 'Orders/ByCompany' select Count ";
            const expected = `from index 'Orders/ByCompany'  select __all_stored_fields`;
            test(query, expected);
        });

        it ('ignores include after select', () => {
            const query = "from index 'Orders/ByCompany' select Count include Seller";
            const expected = `from index 'Orders/ByCompany'  select __all_stored_fields`;
            test(query, expected);
        });

        function test(query: string, expected: string) {
            const result = queryUtil.replaceSelectWithFetchAllStoredFields(query);
            assert.equal(expected, result);
        }

    });
    
    describe('replaceWhereWithDocumentIdPredicate()', () => {

        const ID = 'products/10';

        it ('has where', () => {
            const query = "from Products where X = 1 and price > 40";
            const expected = `from Products where id() = '${ID}'`;
            test(query, expected);
        });

        it ('only from', () => {
            const query = "from Products";
            const expected = `from Products where id() = '${ID}'`;
            test(query, expected);
        });
        
        it ('has update', () => {
            const query = 'from Products update { this.X = 1 }';
            const expected = `from Products where id() = '${ID}' update { this.X = 1 }`;
            test(query, expected);
        });

        it ('has where, update', () => {
            const query = 'from Products where startswith(Desc, "asdf") update { this.X = 1 }';
            const expected = `from Products where id() = '${ID}' update { this.X = 1 }`;
            test(query, expected);
        });

        it ('has where, order by, update', () => {
            const query = 'from Products where startswith(Desc, "asdf") order by UpdatedAt update { this.X = 1 }';
            const expected = `from Products where id() = '${ID}' order by UpdatedAt update { this.X = 1 }`;
            test(query, expected);
        });

        it ('has load', () => {
            const query = 'from Products p load p.Company as c select { }';
            const expected = `from Products p where id() = '${ID}' load p.Company as c select { }`;
            test(query, expected);
        });

        it ('has where, load', () => {
            const query = 'from Products p where p.Price > 155 load p.Company as c select { }';
            const expected = `from Products p where id() = '${ID}' load p.Company as c select { }`;
            test(query, expected);
        });

        it ('has where, load, order by, update', () => {
            const query = 'from Products p where startswith(Desc, "where update") load p.Company as c order by UpdatedAt update { this.X = " where "; }';
            const expected = `from Products p where id() = '${ID}' load p.Company as c order by UpdatedAt update { this.X = " where "; }`;
            test(query, expected);
        });

        it ('multiline and has where, load, order by, update', () => {
            const query = `from Products p
            where startswith(Desc, "where update") 
            load p.Company as c
            order by UpdatedAt
            update {
                this.X = " where ";
            }`;
            const expected = `from Products p where id() = 'products/10' load p.Company as c
            order by UpdatedAt
            update {
                this.X = " where ";
            }`;
            test(query, expected);
        });

        function test(query: string, expected: string) {
            const result = queryUtil.replaceWhereWithDocumentIdPredicate(query, ID);
            assert.equal(expected, result);
        }

    });
});
