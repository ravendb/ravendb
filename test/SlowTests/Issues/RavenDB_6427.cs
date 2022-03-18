using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6427 : RavenTestBase
    {
        public RavenDB_6427(ITestOutputHelper output) : base(output)
        {
        }

        private class Stuff
        {
            public int Key { get; set; }

        }

        [Fact]
        public void CanPatchExactlyOneTime()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    new StuffIndex().Execute(store);

                    // use value over batchSize = 1024
                    for (var i = 0; i < 1030; i++)
                    {
                        bulkInsert.Store(new Stuff
                        {
                            Key = 0
                        });
                    }
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    store.Operations.Send(new PatchByQueryOperation(new IndexQuery() {Query = "FROM Stuffs UPDATE { this.Key = this.Key + 1; }" })).WaitForCompletion(TimeSpan.FromSeconds(15));

                    using (var reader = session.Advanced.Stream<Stuff>(startsWith: "stuffs/"))
                    {
                        while (reader.MoveNext())
                        {
                            var doc = reader.Current.Document;
                            Assert.Equal(1, doc.Key);
                        }
                    }
                }
            }
        }

        private class StuffIndex : AbstractIndexCreationTask<Stuff>
        {
            public StuffIndex()
            {
                Map = entities => from entity in entities
                    select new
                    {
                        entity.Key,
                    };
            }
        }
    }
}
