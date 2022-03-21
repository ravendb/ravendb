using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Voron;

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
