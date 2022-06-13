using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.Tcp;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedDatabaseTcpConnectionInfoHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/info/tcp", "GET")]
    public async Task Get()
    {
        using (var processor = new DatabaseTcpConnectionInfoHandlerProcessorForGet<TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }
}
