using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4011 : RavenNewTestBase
    {
        private class Entity
        {
            public int OrganizationId;
            public long HistoryCode;
            public int CaseId;
        }

        [Fact]
        public void get_index_names()
        {
            using (IDocumentStore store = GetDocumentStore())
            {
                for (int i = 1; i <= 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Entity()
                        {
                            OrganizationId = 1,
                            HistoryCode = 2,
                            CaseId = 3
                        });
                        session.SaveChanges();
                    }
                }

                for (int i = 1; i <= 30; i++)
                {
                    IndexDefinitionBuilder<Entity> builder = new IndexDefinitionBuilder<Entity>
                    {
                        Map = entities => from e in entities
                                          select new
                                          {
                                              Id = e.OrganizationId,
                                              Code = e.HistoryCode,
                                              Case = e.CaseId
                                          }
                    };

                    store.Admin.Send(new PutIndexOperation("TestIndex/Numer" + i, builder.ToIndexDefinition(store.Conventions)));
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Task.Run(() => LoopResetIndex(session)).Wait();
                }
            }
        }

        private static async Task LoopResetIndex(IDocumentSession session)
        {
            for (var i = 0; i < 2; i++)
            {
                var indexNames = session.Advanced.DocumentStore.Admin.Send(new GetIndexNamesOperation(0, 999));
                var cancellation = new CancellationToken();
                await Task.WhenAll(indexNames.Select(t => session.Advanced.DocumentStore.Admin.SendAsync(new ResetIndexOperation(t), cancellation)).ToArray());
            }
        }
    }
}