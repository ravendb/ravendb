using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests.Indexes
{
    public class StoreFields : RavenTestBase
    {
        public StoreFields(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenExplicitData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void QueryForDocumentWithStoreFields(RavenTestParameters configuration)
        {
            UseNewLocalServer();

            using (var store = GetDocumentStore(new Options()
                   {
                       Path = NewDataPath(),
                       ModifyDatabaseRecord = record =>
                       {
                           record.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = configuration.SearchEngine.ToString();
                           record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = configuration.SearchEngine.ToString();
                       }
                   }))
            {
                store.ExecuteIndex(new TestIndex());
                using (var session = store.OpenSession())
                {
                    var item = new Foo {Item1 = "item1", OtherDoc = "id1"};
                    session.Store(item);
                    session.Store(new WannaBeFoo {Item2 = "item2"}, "id1");
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.SaveChanges();
                    var input = session.Advanced.RawQuery<dynamic>(
                        @"from index 'TestIndex' as o
                            load o.OtherDoc as c
                            select  o.item1,
                            c "
                    ).ToList();
                    Assert.NotNull(input);
                }
            }
        }

        private class Foo
        {
            public string Item1 { get; set; }
            public string OtherDoc { get; set; }
        }

        private class WannaBeFoo
        {
            public string Item2 { get; set; }
        }

        private class TestIndex : AbstractIndexCreationTask<Foo, TestIndex.Result>
        {
            public class Result
            {
                public string Item1 { get; set; }
                public string OtherDoc { get; set; }
            }

            public TestIndex()
            {
                Map = docs => from foo in docs
                    select new Result {Item1 = foo.Item1, OtherDoc = foo.OtherDoc};
                Store(x => x.OtherDoc, FieldStorage.Yes);
            }
        }
    }
}
