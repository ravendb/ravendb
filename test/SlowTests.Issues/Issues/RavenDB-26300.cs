using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26300(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void WhenOperatorWithInLong(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Tag = "1", Number = 1 });
        session.Store(new Dto { Tag = "4", Number = 2 });
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
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", 1)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", 4)
                .Count();
            Assert.Equal(2, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .Count();
            Assert.Equal(2, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", (object)null)
                .Count();
            Assert.Equal(2, result);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void WhenOperatorWithInString(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Tag = "aaa", Number = 1 });
        session.Store(new Dto { Tag = "ddd", Number = 2 });
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
                .RawQuery<Dto>($"from {indexName} where when($myVar in ('aaa', 'bbb', 'ccc'), Number != 1)")
                .AddParameter("myVar", "aaa")
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in ('aaa', 'bbb', 'ccc'), Number != 1)")
                .AddParameter("myVar", "AAA")
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in ('aaa', 'bbb', 'ccc'), Number != 1)")
                .AddParameter("myVar", "ddd")
                .Count();
            Assert.Equal(2, result);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void WhenOperatorWithInDouble(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Tag = "1.5", Number = 1 });
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
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1.5, 2.5, 3.5), Number != 1)")
                .AddParameter("myVar", 1.5)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1.5, 2.5, 3.5), Number != 1)")
                .AddParameter("myVar", 4.5)
                .Count();
            Assert.Equal(1, result);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void WhenOperatorWithAllIn(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Tag = "1", Number = 1 });
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
                .RawQuery<Dto>($"from {indexName} where when($myVar all in (1), Number != 1)")
                .AddParameter("myVar", 1)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar all in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", 1)
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar all in ('aaa'), Number != 1)")
                .AddParameter("myVar", "aaa")
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar all in ('aaa', 'bbb'), Number != 1)")
                .AddParameter("myVar", "aaa")
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar all in (1, 2, 3), Number != 1)")
                .Count();
            Assert.Equal(1, result);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void WhenOperatorWithInCombinedWithAndOr(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Tag = "1", Attach = true, Number = 1 });
        session.Store(new Dto { Tag = "1", Attach = true, Number = 2 });
        session.Store(new Dto { Tag = "3", Attach = false, Number = 3 });
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
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2), Number != 1) and Attach == true")
                .AddParameter("myVar", 1)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2), Number != 1) and Attach == true")
                .AddParameter("myVar", 3)
                .Count();
            Assert.Equal(2, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where Number > 0 and not when($myVar in (1, 2), Number == 1)")
                .AddParameter("myVar", 1)
                .Count();
            Assert.Equal(2, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where Number > 0 and not when($myVar in (1, 2), Number == 1)")
                .AddParameter("myVar", 3)
                .Count();
            Assert.Equal(3, result);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void WhenOperatorWithInNull(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Tag = null, Number = 1 });
        session.Store(new Dto { Tag = "aaa", Number = 2 });
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
                .RawQuery<Dto>($"from {indexName} where when($myVar in (null, 1, 2), Number != 1)")
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (null, 1, 2), Number != 1)")
                .AddParameter("myVar", (object)null)
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", (object)null)
                .Count();
            Assert.Equal(2, result);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void WhenOperatorWithInMixedParameterAndValueTypes(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Tag = "1", Number = 1 });
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
                .RawQuery<Dto>($"from {indexName} where when($myVar in ('1', '2', '3'), Number != 1)")
                .AddParameter("myVar", 1L)
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in ('1', '2', '3'), Number != 1)")
                .AddParameter("myVar", 4L)
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", "1")
                .Count();
            Assert.Equal(0, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", 1.0)
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", 1.5)
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1.0, 2.0, 3.0), Number != 1)")
                .AddParameter("myVar", 1L)
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in ('True', 'False'), Number != 1)")
                .AddParameter("myVar", true)
                .Count();
            Assert.Equal(0, result);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public void WhenOperatorWithInArrayParameter(Options options, bool useAutoIndex)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        session.Store(new Dto { Tag = "1", Number = 1 });
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
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", new[] { 1, 5 })
                .WaitForNonStaleResults()
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", new[] { 4, 5 })
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in ('aaa', 'bbb'), Number != 1)")
                .AddParameter("myVar", new[] { "aaa", "zzz" })
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in ('aaa', 'bbb'), Number != 1)")
                .AddParameter("myVar", new[] { "xxx", "zzz" })
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar all in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", new[] { 1, 2 })
                .Count();
            Assert.Equal(0, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar all in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", new[] { 1, 5 })
                .Count();
            Assert.Equal(1, result);
        }

        {
            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", Array.Empty<int>())
                .Count();
            Assert.Equal(1, result);

            result = session.Advanced
                .RawQuery<Dto>($"from {indexName} where when($myVar all in (1, 2, 3), Number != 1)")
                .AddParameter("myVar", Array.Empty<int>())
                .Count();
            Assert.Equal(0, result);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Tag { get; set; }
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
                    dto.Tag, dto.Attach, dto.Number
                };
        }
    }
}
