using System;
using System.IO;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Streaming
{
    internal abstract class AbstractStreamingHandlerProcessor<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStreamingHandlerProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected StreamCsvBlittableQueryResultWriter GetIndexEntriesQueryResultWriter(string format, HttpResponse response, Stream responseBodyStream,
            string[] propertiesArray, string fileNamePrefix = null)
        {
            if (string.IsNullOrEmpty(format) || string.Equals(format, "csv", StringComparison.OrdinalIgnoreCase) == false)
                ThrowUnsupportedException($"You have selected \"{format}\" file format, which is not supported.");

            return new StreamCsvBlittableQueryResultWriter(response, responseBodyStream, propertiesArray, fileNamePrefix);
        }

        protected IStreamQueryResultWriter<Document> GetQueryResultWriter(string format, HttpResponse response, DocumentsOperationContext context, Stream responseBodyStream,
            string[] propertiesArray, string fileNamePrefix = null)
        {
            if (string.IsNullOrEmpty(format) == false && string.Equals(format, "csv", StringComparison.OrdinalIgnoreCase))
            {
                return new StreamCsvDocumentQueryResultWriter(response, responseBodyStream, context, propertiesArray, fileNamePrefix);
            }

            if (propertiesArray != null)
            {
                ThrowUnsupportedException("Using json output format with custom fields is not supported.");
            }

            return new StreamJsonDocumentQueryResultWriter(responseBodyStream, context);
        }

        protected void ThrowUnsupportedException(string message)
        {
            throw new NotSupportedException(message);
        }
    }
}
