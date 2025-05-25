using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

internal abstract class AbstractGetAttachmentStrategyProcessor<TRequestHandler, TOperationContext> : AbstractAttachmentStrategyProcessor<TRequestHandler, TOperationContext>, IGetAttachmentStrategy
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractGetAttachmentStrategyProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public abstract void DisposeReadTransactionIfNeeded(DocumentsTransaction tx);
    public abstract string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name);
    public abstract Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, string collection, CancellationToken token);

    protected async Task WriteAttachmentToResponseStream(DocumentsOperationContext context, Stream stream, CancellationToken token)
    {
        using (context.GetMemoryBuffer(out var buffer))
        {
            var responseStream = RequestHandler.ResponseBodyStream();
            var count = stream.Read(buffer.Memory.Memory.Span); // can never wait, so no need for async
            while (count > 0)
            {
                await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, count), token);
                // we know that this can never wait, so no need to do async i/o here
                count = stream.Read(buffer.Memory.Memory.Span);
            }
        }
    }
}
