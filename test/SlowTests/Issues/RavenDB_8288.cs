using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8288 : RavenTestBase
    {
        public RavenDB_8288(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Queries_will_work_during_index_replacements()
        {
            using (var store = GetDocumentStore(new Options
            {
                Path = NewDataPath()
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe" });
                    await session.StoreAsync(new User { Name = "Doe" });

                    await session.SaveChangesAsync();
                }

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                }));

                var changeDefinitionTask = Task.Factory.StartNew(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                        {
                            Maps =
                            {
                                "from user in docs.Users select new { Name" + i + "= user.Name }"
                            },
                            Type = IndexType.Map,
                            Name = "Users_ByName"
                        }));

                        Thread.Sleep(100);
                    }
                });


                while (changeDefinitionTask.IsCompleted == false)
                {
                    Parallel.For(0, 4, _ =>
                    {
                        using (var session = store.OpenSession())
                        {
                            var count = session.Query<User>("Users_ByName").Customize(x => x.WaitForNonStaleResults()).Count();
                            Assert.Equal(2, count);

                            Thread.Sleep(13);
                        }
                    });
                }
            }
        }
    }
}
