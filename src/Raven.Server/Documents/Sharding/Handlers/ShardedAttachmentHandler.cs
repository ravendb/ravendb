using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Attachments;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedAttachmentHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/attachments", "DELETE")]
        public async Task Delete()
        {
            using (var processor = new ShardedAttachmentsHandlerProcessorForDeleteAttachment(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
