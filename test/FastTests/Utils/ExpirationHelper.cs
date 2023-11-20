using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Server.Documents.Commands;

namespace FastTests.Utils
{
    public static class ExpirationHelper
    {
        public static async Task SetupExpiration(IDocumentStore store, Raven.Server.ServerWide.ServerStore serverStore, ExpirationConfiguration configuration)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));
            if (serverStore == null)
                throw new ArgumentNullException(nameof(serverStore));
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var result = await store.Maintenance.SendAsync(new ConfigureExpirationOperation(configuration));

            var documentDatabase = await serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(result.RaftCommandIndex.Value, serverStore.Engine.OperationTimeout);
        }

        public static async Task SetupExpirationAsync(IDocumentStore store, ExpirationConfiguration configuration)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var result = await store.Maintenance.SendAsync(new ConfigureExpirationOperation(configuration));

            await store.Maintenance.SendAsync(new WaitForIndexNotificationOperation(result.RaftCommandIndex.Value));
        }
    }
}
