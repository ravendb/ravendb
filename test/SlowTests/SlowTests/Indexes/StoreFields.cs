using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SlowTests.Indexes
{
    public class StoreFields : RavenTestBase
    {
        [Fact]
        public void QueryForDocumentWithStoreFields()
        {
            UseNewLocalServer();

            using (var store = GetDocumentStore(new Options() { Path = NewDataPath() }))
            {
                store.ExecuteIndex(new TestIndex());
                using (var session = store.OpenSession())
                {
                    var item = new Foo { Item1 = "item1", OtherDoc = "id1" };
                    session.Store(item);
                    session.Store(new WannaBeFoo { Item2 = "item2" }, "id1");
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
                              select new Result
                              {
                                  Item1 = foo.Item1,
                                  OtherDoc = foo.OtherDoc
                              };
                Store(x => x.OtherDoc, FieldStorage.Yes);
            }
        }
    }
}
