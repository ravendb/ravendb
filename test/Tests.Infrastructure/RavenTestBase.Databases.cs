using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Web;
using Raven.Server.Web.System;
using Raven.Server.Web.System.Processors.OngoingTasks;
using Voron;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly DatabasesTestBase Databases;

    public class DatabasesTestBase
    {
        private readonly RavenTestBase _parent;

        public DatabasesTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(IDocumentStore store, string database = null)
        {
            return _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database ?? store.Database);
        }

        public async Task SetDatabaseId(DocumentStore store, Guid dbId)
        {
            var database = await GetDocumentDatabaseInstanceFor(store);
            var type = database.GetAllStoragesEnvironment().Single(t => t.Type == StorageEnvironmentWithType.StorageEnvironmentType.Documents);
            type.Environment.FillBase64Id(dbId);
        }

        public Task<List<OngoingTask>> GetOngoingTasks(string database, RavenServer server) =>
            GetOngoingTasks(database, new List<RavenServer>(capacity: 1) { server });

        public async Task<List<OngoingTask>> GetOngoingTasks(string database, List<RavenServer> servers)
        {
            var tasks = new Dictionary<long, OngoingTask>();
            foreach (var server in servers)
            {
                using (var processor = await InstantiateOutgoingTaskProcessor(database, server))
                {
                    foreach (var task in processor.GetOngoingTasksInternal().OngoingTasksList)
                    {
                        if (tasks.ContainsKey(task.TaskId) == false && task.TaskConnectionStatus != OngoingTaskConnectionStatus.NotOnThisNode)
                            tasks.Add(task.TaskId, task);
                    }
                }
            }
            return tasks.Values.ToList();
        }

        internal async Task<OngoingTasksHandlerProcessorForGetOngoingTasks> InstantiateOutgoingTaskProcessor(string name, RavenServer server)
        {
            Assert.True(server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(name, out var db));
            var database = await db;
            var handler = new OngoingTasksHandler();
            var ctx = new RequestHandlerContext
            {
                RavenServer = server,
                Database = database,
                HttpContext = new DefaultHttpContext()
            };
            handler.Init(ctx);
            return new OngoingTasksHandlerProcessorForGetOngoingTasks(handler);
        }

        public IDisposable EnsureDatabaseDeletion(string databaseToDelete, IDocumentStore store)
        {
            return new DisposableAction(() =>
            {
                try
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseToDelete, hardDelete: true));
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Failed to delete '{databaseToDelete}' database. Exception: " + e);

                    // do not throw to not hide an exception that could be thrown in a test
                }
            });
        }
    }
}
