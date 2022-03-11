using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public partial class ShardedRequestHandler
    {
        public class ShardedContinuationTokensHandler
        {
            private readonly ShardedRequestHandler _handler;

            public ShardedContinuationTokensHandler(ShardedRequestHandler handler)
            {
                _handler = handler;
            }

            public ShardedPagingContinuation GetOrCreateContinuationToken(JsonOperationContext context)
            {
                var qToken = _handler.GetStringQueryString(ContinuationToken.ContinuationTokenQueryString, required: false);
                var token = ContinuationToken.FromBase64<ShardedPagingContinuation>(context, qToken) ??
                            new ShardedPagingContinuation(_handler.ShardedContext, _handler.GetStart(), _handler.GetPageSize());
                return token;
            }
        }
    }
}
