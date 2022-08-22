using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class ReplicationHandlerProcessorForGetTombstones : AbstractReplicationHandlerProcessorForGetTombstones<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetTombstones([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<GetTombstonesPreviewResult> GetTombstonesAsync(DocumentsOperationContext context, int start, int pageSize)
        {
            using (context.OpenReadTransaction())
            {
                var tombstones = context.DocumentDatabase.DocumentsStorage.GetTombstonesPreviewResult(context, 0, start, pageSize);
                return ValueTask.FromResult(tombstones);
            }
        }
    }
}
