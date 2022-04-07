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
            using (var processor = new ShardedAttachmentHandlerProcessorForDeleteAttachment(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/attachments", "PUT")]
        public async Task Put()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForPutAttachment(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/attachments", "GET")]
        public async Task Get()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForGetAttachment(this, isDocument: true))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/attachments", "POST")]
        public async Task GetPost()
        {
            using (var processor = new ShardedAttachmentHandlerProcessorForGetAttachment(this, isDocument: false))
                await processor.ExecuteAsync();
        }
    }
}
