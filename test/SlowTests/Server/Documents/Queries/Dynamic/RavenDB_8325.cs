using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Queries.Dynamic
{
    public class RavenDB_8325 : RavenTestBase
    {
        public RavenDB_8325(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public long Value { get; set; }
        }

        [Fact]
        public void Numeric_values_should_to_be_sorted_lexically_if_not_specified_explicitly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item() { Value = 1 });
                    session.Store(new Item() { Value = 2 });
                    session.Store(new Item() { Value = 10 });

                    session.SaveChanges();

                    var items = session.Advanced.RawQuery<Item>("from Items where Value >= 0 order by Value").ToList();

                    Assert.Equal(1, items[0].Value);
                    Assert.Equal(10, items[1].Value);
                    Assert.Equal(2, items[2].Value);

                    using (var cmds = store.Commands())
                    {
                        var command = new QueryCommand(cmds.Session, new IndexQuery
                        {
                            Query = "from Items where Value >= $p0 order by Value",
                            QueryParameters = new Parameters()
                            {
                                { "p0", 0 }
                            }
                        });

                        cmds.RequestExecutor.Execute(command, cmds.Context);

                        Assert.Equal(1, (long)((BlittableJsonReaderObject)command.Result.Results[0])["Value"]);
                        Assert.Equal(10, (long)((BlittableJsonReaderObject)command.Result.Results[1])["Value"]);
                        Assert.Equal(2, (long)((BlittableJsonReaderObject)command.Result.Results[2])["Value"]);
                    }
                }
            }
        }
    }
}
