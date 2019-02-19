using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4011 : RavenTestBase
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
                    var indexDefinition = builder.ToIndexDefinition(store.Conventions);
                    indexDefinition.Name = "TestIndex/Numer" + i;
                    store.Maintenance.Send(new PutIndexesOperation(new [] {indexDefinition}));
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
                var indexNames = session.Advanced.DocumentStore.Maintenance.Send(new GetIndexNamesOperation(0, 999));
                var cancellation = new CancellationToken();
                await Task.WhenAll(indexNames.Select(t => session.Advanced.DocumentStore.Maintenance.SendAsync(new ResetIndexOperation(t), cancellation)).ToArray());
            }
        }
    }
}