using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Client.Commands;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Data.Indexes;
using Raven.NewClient.Client;

namespace NewClientTests.NewClient.Client.Indexing
{
    public class IndexesDeleteByIndexTests : RavenTestBase
    {

        [Fact]
        public void Delete_By_Index()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Fitzchak"});
                    session.Store(new User {Name = "Arek"});
                    session.SaveChanges();
                }

                string indexName;
                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var people = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(x => x.Name == "Arek")
                        .ToList();

                    indexName = stats.IndexName;
                }

                JsonOperationContext context;
                store.GetRequestExecuter(store.DefaultDatabase).ContextPool.AllocateOperationContext(out context);

                DeleteByIndex(context, indexName, store);
                var databaseStatistics = DatabaseStatistics(store, context);

                Assert.Equal(1, databaseStatistics.CountOfDocuments);
            }
        }

        [Fact]
        public async void Delete_By_Index_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" }, "users/1");
                    await session.StoreAsync(new User { Name = "Arek" }, "users/2");
                    await session.SaveChangesAsync();
                }

                string indexName;
                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var people = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out stats)
                        .Where(x => x.Name == "Arek")
                        .ToList();

                    indexName = stats.IndexName;
                }

                JsonOperationContext context;
                store.GetRequestExecuter(store.DefaultDatabase).ContextPool.AllocateOperationContext(out context);

               
                var deleteByIndexOperation = new DeleteByIndexOperation(context);
                var command = deleteByIndexOperation.CreateRequest(indexName, new IndexQuery(),
                    new QueryOperationOptions { AllowStale = false }, store);

                if (command != null)
                    await store.GetRequestExecuter(store.DefaultDatabase).ExecuteAsync(command, context);
                var databaseStatistics = DatabaseStatistics(store, context);
                
                //TODO - after we have hilo need to be 1
                Assert.Equal(0, databaseStatistics.CountOfDocuments);
            }
        }

        private static DatabaseStatistics DatabaseStatistics(DocumentStore store, JsonOperationContext context)
        {
            var getStatsCommand = new GetStatisticsCommand();
            store.GetRequestExecuter(store.DefaultDatabase).Execute(getStatsCommand, context);
            var databaseStatistics = getStatsCommand.Result;
            return databaseStatistics;
        }

        private static void DeleteByIndex(JsonOperationContext context, string indexName, DocumentStore store)
        {
            var deleteByIndexOperation = new DeleteByIndexOperation(context);
            var command = deleteByIndexOperation.CreateRequest(indexName, new IndexQuery(),
                new QueryOperationOptions {AllowStale = false}, store);

            if (command != null)
                store.GetRequestExecuter(store.DefaultDatabase).Execute(command, context);
        }
    }
}