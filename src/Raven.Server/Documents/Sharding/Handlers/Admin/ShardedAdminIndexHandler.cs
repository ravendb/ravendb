using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Indexes;
using Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    public class ShardedAdminIndexHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/indexes", "PUT")]
        public async Task Put()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForStaticPut(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes", "PUT")]
        public async Task PutJavaScript()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForJavaScriptPut(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/indexes/stop", "POST")]
        public async Task Stop()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForStop(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/indexes/start", "POST")]
        public async Task Start()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForStart(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/indexes/enable", "POST")]
        public async Task Enable()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForState(IndexState.Normal, this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/indexes/disable", "POST")]
        public async Task Disable()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForState(IndexState.Disabled, this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/indexes/dump", "POST")]
        public async Task Dump()
        {
            using (var processor = new ShardedAdminIndexHandlerProcessorForDump(this))
                await processor.ExecuteAsync();
        }
    }
}
