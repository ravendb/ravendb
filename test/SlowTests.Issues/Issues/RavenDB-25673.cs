using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25673(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void WhenOperatorNullTest(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Attach = true, Number = 1 });
        session.SaveChanges();

        var indexName = useAutoIndex ? "Dtos" : "index 'Index'";

        if (useAutoIndex == false)
        {
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
        }


        int result;
        // NotExistingParameter
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == null, Number != 1)")
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != null, Number != 1)")
                .Count();
            Assert.Equal(1, result);

            foreach (var op in new[] { ">", ">=", "<", "<=" })
            {
                result = session.Advanced
                    .RawQuery<Dto>($"from {indexName} where when($myVar {op} null, Number != 1)")
                    .Count();
                Assert.Equal(1, result);
            }

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 1.5, Number != 1)")
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 1, Number != 1)")
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != true, Number != 1)")
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 'test', Number != 1)")
                .Count();
            Assert.Equal(0, result);

            foreach (var value in new[] { "1.5", "1", "true", "'test'" })
            foreach (var op in new[] { ">", ">=", "<", "<=" })
            {
                var query = $"from {indexName} where when($myVar {op} {value}, Number != 1)";
                result = session.Advanced
                    .RawQuery<Dto>(query)
                    .Count();
                Assert.Equal(1, result);
            }
        }

        // Null parameter
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == null, Number != 1)")
                .AddParameter("myVar", null)
                .Count();
            Assert.Equal(0, result);

            foreach (var value in new[] { "1.5", "1", "true", "'test'" })
            {
                result = session.Advanced
                    .RawQuery<Dto>($"from {indexName} where when($myVar != {value}, Number != 1)")
                    .AddParameter("myVar", null)
                    .Count();
                Assert.Equal(0, result);
            }

            foreach (var op in new[] { "!=", ">", ">=", "<", "<=" })
            {
                result = session.Advanced
                    .RawQuery<Dto>($"from {indexName} where when($myVar {op} null, Number != 1)")
                    .AddParameter("myVar", null)
                    .Count();
                Assert.Equal(1, result);
            }

            foreach (var value in new[] { "1.5", "1", "true", "'test'" })
            foreach (var op in new[] { ">", ">=", "<", "<=" })
            {
                result = session.Advanced
                    .RawQuery<Dto>($"from {indexName} where when($myVar {op} {value}, Number != 1)")
                    .AddParameter("myVar", null)
                    .Count();
                Assert.Equal(1, result);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void ConstantExpressionDouble(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Attach = true, Number = 1 });
        session.SaveChanges();

        var indexName = useAutoIndex ? "Dtos" : "index 'Index'";
        if (useAutoIndex == false)
        {
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        int result;

        //double to double
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 1.5, Number != 1)")
                .AddParameter("myVar", 1.5)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 1.5, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 1.5, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 1.5, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 1.5, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 1.5, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count();
            Assert.Equal(0, result);
        }

        //long to double
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 2.0, Number != 1)")
                .AddParameter("myVar", 2L)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 2.0, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 1.5, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 2.0, Number != 1)")
                .AddParameter("myVar", 2L)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 1.5, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 1.5, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 1.5, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 1.5, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 1.5, Number != 1)")
                .AddParameter("myVar", 2L)
                .Count();
            Assert.Equal(0, result);
        }

        //bool <-> double
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 1.5, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 1.5, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 1.5, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 1.5, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 1.5, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 1.5, Number != 1)")
                .AddParameter("myVar", true)
                .Count());
        }

        //string <-> double
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 1.5, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 1.5, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 1.5, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 1.5, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 1.5, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 1.5, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void ConstantExpressionLong(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Attach = true, Number = 1 });
        session.SaveChanges();

        var indexName = useAutoIndex ? "Dtos" : "index 'Index'";
        if (useAutoIndex == false)
        {
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        int result;

        //long to long
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 10, Number != 1)")
                .AddParameter("myVar", 10L)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 10, Number != 1)")
                .AddParameter("myVar", 10L)
                .Count();
            Assert.Equal(1, result);
            
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 1_0, Number != 1)")
                .AddParameter("myVar", 1_0L)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 10, Number != 1)")
                .AddParameter("myVar", 10L)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 10, Number != 1)")
                .AddParameter("myVar", 10L)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 10, Number != 1)")
                .AddParameter("myVar", 10L)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 10, Number != 1)")
                .AddParameter("myVar", 10L)
                .Count();
            Assert.Equal(0, result);
        }

        //double to long
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 10, Number != 1)")
                .AddParameter("myVar", 10.0)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 10, Number != 1)")
                .AddParameter("myVar", 5.5)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 10, Number != 1)")
                .AddParameter("myVar", 5.5)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 10, Number != 1)")
                .AddParameter("myVar", 10.0)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 10, Number != 1)")
                .AddParameter("myVar", 10.0)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 10, Number != 1)")
                .AddParameter("myVar", 10.0)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 10, Number != 1)")
                .AddParameter("myVar", 10.0)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 10, Number != 1)")
                .AddParameter("myVar", 10.0)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 10, Number != 1)")
                .AddParameter("myVar", 9.5)
                .Count();
            Assert.Equal(1, result);
        }

        //bool <-> long
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 10, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 10, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 10, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 10, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 10, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 10, Number != 1)")
                .AddParameter("myVar", true)
                .Count());
        }

        //string <-> long
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 10, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 10, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 10, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 10, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 10, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 10, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void ConstantExpressionString(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Attach = true, Number = 1 });
        session.SaveChanges();

        var indexName = useAutoIndex ? "Dtos" : "index 'Index'";
        if (useAutoIndex == false)
        {
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        int result;

        //string to string
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 'bbb', Number != 1)")
                .AddParameter("myVar", "bbb")
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 'BBB', Number != 1)")
                .AddParameter("myVar", "bbb")
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);
            
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 'bbb', Number != 1)")
                .AddParameter("myVar", "BbB")
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);
            
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 'BBB', Number != 1)")
                .AddParameter("myVar", "bbb")
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);
            
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 'bbb', Number != 1)")
                .AddParameter("myVar", "aaa")
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 'bbb', Number != 1)")
                .AddParameter("myVar", "aaa")
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 'bbb', Number != 1)")
                .AddParameter("myVar", "bbb")
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 'bbb', Number != 1)")
                .AddParameter("myVar", "aaa")
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 'bbb', Number != 1)")
                .AddParameter("myVar", "aaa")
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 'bbb', Number != 1)")
                .AddParameter("myVar", "aaa")
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 'bbb', Number != 1)")
                .AddParameter("myVar", "aaa")
                .Count();
            Assert.Equal(1, result);
        }

        //long <-> string
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 'bbb', Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 'bbb', Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 'bbb', Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 'bbb', Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 'bbb', Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 'bbb', Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());
        }

        //double <-> string
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 'bbb', Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 'bbb', Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 'bbb', Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 'bbb', Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 'bbb', Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 'bbb', Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());
        }

        //bool <-> string
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == 'bbb', Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != 'bbb', Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < 'bbb', Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= 'bbb', Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > 'bbb', Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= 'bbb', Number != 1)")
                .AddParameter("myVar", true)
                .Count());
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void ConstantExpressionBool(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Attach = true, Number = 1 });
        session.SaveChanges();

        var indexName = useAutoIndex ? "Dtos" : "index 'Index'";
        if (useAutoIndex == false)
        {
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        int result;

        //bool to bool
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == true, Number != 1)")
                .AddParameter("myVar", true)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == true, Number != 1)")
                .AddParameter("myVar", false)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != true, Number != 1)")
                .AddParameter("myVar", false)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != true, Number != 1)")
                .AddParameter("myVar", true)
                .Count();
            Assert.Equal(1, result);

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < true, Number != 1)")
                .AddParameter("myVar", false)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= true, Number != 1)")
                .AddParameter("myVar", false)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > false, Number != 1)")
                .AddParameter("myVar", true)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= false, Number != 1)")
                .AddParameter("myVar", true)
                .Count());
        }

        //long <-> bool
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == true, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != true, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < true, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= true, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > true, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= true, Number != 1)")
                .AddParameter("myVar", 1L)
                .Count());
        }

        //double <-> bool
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == true, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != true, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < true, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= true, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > true, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= true, Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count());
        }

        //string <-> bool
        {
            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar == true, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar != true, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar < true, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar <= true, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar > true, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());

            Assert.Throws<RavenException>(() => session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar >= true, Number != 1)")
                .AddParameter("myVar", "abc")
                .Count());
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void ConstantExpressionLogical(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Attach = true, Number = 1 });
        session.SaveChanges();

        var indexName = useAutoIndex ? "Dtos" : "index 'Index'";
        if (useAutoIndex == false)
        {
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        int result;

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($a > 10 and $b < 20, Number != 1)")
                .AddParameter("a", 15)
                .AddParameter("b", 10)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($a > 10 and $b < 20, Number != 1)")
                .AddParameter("a", 15)
                .AddParameter("b", 25)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($a > 10 and $b < 20, Number != 1)")
                .AddParameter("a", 5)
                .AddParameter("b", 10)
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($a > 10 or $b < 20, Number != 1)")
                .AddParameter("a", 15)
                .AddParameter("b", 25)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($a > 10 or $b < 20, Number != 1)")
                .AddParameter("a", 5)
                .AddParameter("b", 10)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($a > 10 or $b < 20, Number != 1)")
                .AddParameter("a", 5)
                .AddParameter("b", 25)
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 100 and $flag == true, Number != 1)")
                .AddParameter("val", 100L)
                .AddParameter("flag", true)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 100 and $flag == true, Number != 1)")
                .AddParameter("val", 100L)
                .AddParameter("flag", false)
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when(($a == 1 and $b == 1) or $c == true, Number != 1)")
                .AddParameter("a", 0)
                .AddParameter("b", 0)
                .AddParameter("c", true)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when(($a == 1 and $b == 1) or $c == true, Number != 1)")
                .AddParameter("a", 0)
                .AddParameter("b", 1)
                .AddParameter("c", false)
                .Count();
            Assert.Equal(1, result);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single, Data = new object[] { false })]
    public void BinaryOperations(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;

        session.Store(new Dto { Attach = true, Number = 1 }, "items/1");
        session.Store(new Dto { Attach = true, Number = 2 }, "items/2");
        session.Store(new Dto { Attach = true, Number = 3 }, "items/3");
        session.SaveChanges();

        var indexName = useAutoIndex ? "Dtos" : "index 'Index'";
        if (useAutoIndex == false)
        {
            new Index().Execute(store);
            Indexes.WaitForIndexing(store);
        }

        int result;

        // AND operation
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 1, Number == 1) and Number < 2")
                .AddParameter("val", 1)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 2, Number == 1) and Number < 2")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 1, Number == 2) and Number < 2")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(0, result);
        }

        // OR operation
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 1, Number == 1) or Number == 3")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(2, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 2, Number == 1) or Number == 3")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(1, result);
        }

        // Multiple WHENs
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($a == 1, Number > 1) and when($b == 1, Number < 3)")
                .AddParameter("a", 1)
                .AddParameter("b", 1)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($a == 2, Number > 1) and when($b == 1, Number < 3)")
                .AddParameter("a", 1)
                .AddParameter("b", 1)
                .Count();
            Assert.Equal(2, result);
        }

        // AND NOT operation
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 1, Number > 1) and not Number == 3")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 1, Number == 1) and not Number == 3")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($val == 2, Number == 1) and not Number == 3")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(2, result);
        }

        // Condition AND NOT When(XYZ)
        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where Number > 1 and not when($val == 1, Number == 3)")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where Number > 1 and not when($val == 2, Number == 3)")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(2, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where Number < 3 and not when($val == 1, Number == 1)")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(1, result);
            
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where Attach == true and not when($val == 1, Number == 2)")
                .AddParameter("val", 1)
                .Count();
            Assert.Equal(2, result);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public bool Attach { get; set; }
        public int Number { get; set; }
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from dto in dtos
                select new
                {
                    dto.Attach, dto.Number
                };
        }
    }
}
