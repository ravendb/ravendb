using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.BackgroundWork;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.NotificationCenter.BackgroundWork;

public class ShardedDatabaseStatsSender : AbstractDatabaseStatsSender
{
    private readonly ShardedDatabaseContext _context;

    public ShardedDatabaseStatsSender([NotNull] ShardedDatabaseContext context, [NotNull] ShardedDatabaseNotificationCenter notificationCenter)
        : base(context.DatabaseName, notificationCenter, context.DatabaseShutdown)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
    }

    protected override async ValueTask<NotificationCenterDatabaseStats> GetStatsAsync()
    {
        return await _context.ShardExecutor.ExecuteParallelForAllAsync(new GetNotificationCenterDatabaseStatsOperation());
    }

    private struct GetNotificationCenterDatabaseStatsOperation : IShardedOperation<NotificationCenterDatabaseStats>
    {
        public HttpRequest HttpRequest => null;

        public NotificationCenterDatabaseStats Combine(Memory<NotificationCenterDatabaseStats> results)
        {
            var result = new NotificationCenterDatabaseStats();

            for (var i = 0; i < results.Length; i++)
            {
                var stats = results.Span[i];

                result.CombineWith(stats);
            }

            return result;
        }

        public RavenCommand<NotificationCenterDatabaseStats> CreateCommandForShard(int shardNumber) => new GetNotificationCenterDatabaseStatsCommand();

        private class GetNotificationCenterDatabaseStatsCommand : RavenCommand<NotificationCenterDatabaseStats>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/notification-center/stats";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationServer.NotificationCenterDatabaseStats(response);
            }
        }
    }
}
