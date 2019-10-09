using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Refresh;

namespace FastTests.Utils
{
    public static class RefreshHelper
    {
        public static async Task SetupExpiration(IDocumentStore store, Raven.Server.ServerWide.ServerStore serverStore, RefreshConfiguration configuration)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));
            if (serverStore == null)
                throw new ArgumentNullException(nameof(serverStore));
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var result = await store.Maintenance.SendAsync(new ConfigureRefreshOperation(configuration));

            var documentDatabase = await serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(result.RaftCommandIndex.Value, serverStore.Engine.OperationTimeout);
        }
    }
}
