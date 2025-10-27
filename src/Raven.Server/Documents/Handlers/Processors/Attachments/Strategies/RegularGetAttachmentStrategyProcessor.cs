using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal sealed class RegularGetAttachmentStrategyProcessor : AbstractGetAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RegularGetAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            // noop
        }

        public override void CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            // noop
        }

        public override async Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, OperationCancelToken tcs)
        {
            var (sendBody, start, bytesRemaining) = RangeHelper.SetRangeHeaders(HttpContext, attachment.Size);
            if (!sendBody)
                return;

            await using (var stream = attachment.Stream)
            {
                if (start > 0)
                {
                    stream.Seek(start, SeekOrigin.Begin);
                }

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

                        await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, read), tcs.Token);
                    }
                }
            }
        }
    }
}
