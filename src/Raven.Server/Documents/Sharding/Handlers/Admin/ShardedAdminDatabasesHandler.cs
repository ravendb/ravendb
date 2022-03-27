using System;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Utils;
using Raven.Server.Web;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    public class ShardedAdminDatabasesHandler : RequestHandler
    {
        [RavenAction("/admin/databases/sharded/wait-for", "POST", AuthorizationStatus.Operator)]
        public async Task WaitFor()
        {
            var database = GetStringQueryString("database");
            var index = GetLongQueryString("index");

            var result = Server.ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(database);
            if (result.DatabaseStatus != DatabasesLandlord.DatabaseSearchResult.Status.Sharded)
                throw new NotSupportedException("This endpoint is only for sharded database");

            await result.DatabaseContext.ShardedDatabaseContextIndexNotifications.WaitForIndexNotification(index, HttpContext.RequestAborted);

            for (int i = 0; i < result.DatabaseContext.ShardCount; i++)
            {
                var name = ShardHelper.ToShardName(database, i);
                if (Server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(name, out var task))
                {
                    var physicalDatabase = await task;
                    await physicalDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, HttpContext.RequestAborted);
                }
            }
        }
    }
}
