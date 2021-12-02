
/*

//TODO: migrate

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
