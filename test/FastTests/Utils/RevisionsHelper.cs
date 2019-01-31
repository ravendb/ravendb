using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Sparrow.Json;

namespace FastTests.Utils
{
    public class RevisionsHelper
    {
        public static async Task SetupRevisions(IDocumentStore store, Raven.Server.ServerWide.ServerStore serverStore, RevisionsConfiguration configuration)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));
            if (serverStore == null)
                throw new ArgumentNullException(nameof(serverStore));
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

            var documentDatabase = await serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(result.RaftCommandIndex.Value, serverStore.Engine.OperationTimeout);
        }

        public static async Task<long> SetupRevisions(Raven.Server.ServerWide.ServerStore serverStore, string database, Action<RevisionsConfiguration> modifyConfiguration = null)
        {
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 5
                },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true,
                        MinimumRevisionsToKeep = 123
                    },
                    ["People"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 10
                    },
                    ["Comments"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = true
                    },
                    ["Products"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = true
                    }
                }
            };

            modifyConfiguration?.Invoke(configuration);

            var index = await SetupRevisions(serverStore, database, configuration);

            var documentDatabase = await serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, serverStore.Engine.OperationTimeout);

            return index;
        }

        private static async Task<long> SetupRevisions(Raven.Server.ServerWide.ServerStore serverStore, string database, RevisionsConfiguration configuration)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configurationJson = EntityToBlittable.ConvertCommandToBlittable(configuration, context);
                var (index, _) = await serverStore.ModifyDatabaseRevisions(context, database, configurationJson);
                return index;
            }
        }
    }
}
