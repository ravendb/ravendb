using FastTests;
using Raven.Client;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_7996 : RavenTestBase
    {
        public RavenDB_7996(ITestOutputHelper output) : base(output)
        {
        }

        private class Company
        {
            public Address Address { get; set; }
        }

        [Fact]
        public void Can_retrieve_index_entries_of_auto_map_reduce_index()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        s.Store(new Company
                        {
                            Address = new Address
                            {
                                Country = "UK"
                            }
                        });
                    }
                    s.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    var query = @"
                        from Companies
                        group by Address.Country
                        select count(), Address.Country";

                    var results = commands.Query(new IndexQuery { Query = query }).Results;

                    Assert.Equal(1, results.Length);

                    var entriesOnly = commands.Query(new IndexQuery { Query = query }, indexEntriesOnly: true).Results;

                    Assert.Equal(1, entriesOnly.Length);

                    var item = (BlittableJsonReaderObject)entriesOnly[0];

                    Assert.Equal(3, item.Count);

                    Assert.True(item.TryGet("Address.Country", out string country));
                    Assert.Equal("uk", country);

                    Assert.True(item.TryGet("Count", out long count));
                    Assert.Equal(5, count);

                    Assert.True(item.TryGet(Constants.Documents.Indexing.Fields.ReduceKeyHashFieldName, out object _));
                }
            }
        }
    }
}
