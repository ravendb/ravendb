using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12250 : RavenTestBase
    {
        [Fact]
        public void StringTypeCheckShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var json = commands.ParseJson(@"
{
    ""Names"": [
        ""5"",
        {
            ""MyField"": 3
        }
    ],
    ""@metadata"": {
        ""@collection"": ""Users""
    }
}");

                    commands.Put("users/1", null, json);
                }

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Index1",
                    Maps =
                    {
                        @"
from doc in docs.Users
let c = doc.Names.Where(x => x is string).Count
where c > 0
select new
{
    doc.Id
}"
                    }
                }));

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var items = session.Query<dynamic>("Index1")
                        .ToList();

                    Assert.Equal(1, items.Count);
                }
            }
        }
    }
}
