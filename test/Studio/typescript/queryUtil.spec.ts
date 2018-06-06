/// <reference path="../../../src/Raven.Studio/typings/tsd.d.ts" />

import chai = require("chai");
const assert = chai.assert;

import queryUtil = require("src/Raven.Studio/typescript/common/queryUtil");

describe("queryUtil", () => {


    describe('replaceSelectAndIncludeWithFetchAllStoredFields()', () => {

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
            const result = queryUtil.replaceSelectAndIncludeWithFetchAllStoredFields(query);
            assert.equal(expected, result);
        }

    });
    
});
