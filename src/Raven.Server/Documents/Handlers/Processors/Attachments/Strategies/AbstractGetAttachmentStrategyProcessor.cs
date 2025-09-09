using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
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
    public abstract void CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name);
    public abstract Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, OperationCancelToken token);

    protected async Task WriteAttachmentToResponseStream(DocumentsOperationContext context, Stream stream, Attachment attachment, long? bytesRemaining,
        CancellationToken token)
    {
        using (context.GetMemoryBuffer(out var buffer))
        {
            var responseStream = RequestHandler.ResponseBodyStream();
            while (true)
            {
                if (bytesRemaining is <= 0)
                {
                    return;
                }

                var readLength = buffer.Size;
                if (bytesRemaining.HasValue)
                {
                    readLength = (int)Math.Min(bytesRemaining.Value, readLength);
                }

                var read = stream.Read(buffer.Memory.Memory.Span.Slice(0, readLength)); // can never wait, so no need for async

                if (bytesRemaining.HasValue)
                {
                    bytesRemaining -= read;
                }

                // End of the source stream.
                if (read == 0)
                {
                    return;
                }

                await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, read), token);
            }
        }
    }
}
