using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12301 : RavenTestBase
    {
        [Fact]
        public void ShouldCompileAndWork()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Index1",
                    Maps =
                    {
                        @"
from doc in docs.Users
where doc.Name.Contains(""missing"") || (doc.FirstName.Contains(""missing"") || doc.LastName.Contains(""missing""))
select new {
    doc.Id
}"
                    }
                }));

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Index2",
                    Maps =
                    {
                        @"docs.Users.Where(doc => doc.Name.Contains(""missing"") || (doc.FirstName.Contains(""missing"") || doc.LastName.Contains(""missing""))).Select(doc => new { doc.Id });"
                    }
                }));

                using (var commands = store.Commands())
                {
                    commands.Put("users/1", null, new
                    {
                        Name = "John"
                    }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Users"}
                    });

                    commands.Put("users/2", null, new
                    {
                        LastName = "missing"
                    }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Users"}
                    });
                }

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<User>("Index1")
                        .Count();

                    Assert.Equal(1, count);

                    count = session.Query<User>("Index2")
                        .Count();

                    Assert.Equal(1, count);
                }
            }
        }
    }
}
