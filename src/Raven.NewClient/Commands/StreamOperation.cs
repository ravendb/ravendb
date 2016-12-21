using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Document;
using Sparrow.Logging;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Linq;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands
{
    public class StreamOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<StreamOperation>("Raven.NewClient.Client");

        public StreamOperation(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        protected void LogStream()
        {
           //TODO
        }

        public StreamCommand CreateRequest<T>(IDocumentQuery<T> query)
        {
            var ravenQueryInspector = ((IRavenQueryInspector)query);
            var indexQuery = ravenQueryInspector.GetIndexQuery(false);
            if (indexQuery.WaitForNonStaleResults || indexQuery.WaitForNonStaleResultsAsOfNow)
                throw new NotSupportedException(
                    "Since Stream() does not wait for indexing (by design), streaming query with WaitForNonStaleResults is not supported.");
            _session.IncrementRequestCount();
            var reference = new Reference<QueryHeaderInformation>();
            var index = ravenQueryInspector.IndexQueried;
            if (string.IsNullOrEmpty(index))
                throw new ArgumentException("Key cannot be null or empty index");
            string path;
            if (indexQuery.Query != null && indexQuery.Query.Length > _session.Conventions.MaxLengthOfQueryUsingGetUrl)
            {
                path = indexQuery.GetIndexQueryUrl(index, "streams/queries", includePageSizeEvenIfNotExplicitlySet: false, includeQuery: false);
            }
            else
            {
                path = indexQuery.GetIndexQueryUrl(index, "streams/queries", includePageSizeEvenIfNotExplicitlySet: false);
            }

            return new StreamCommand()
            {
                Index = path,
            };
        }

        public IEnumerator<BlittableJsonReaderObject> SetResult<T>(Stream result)
        {
            //WIP
            using (var parser = new JsonOperationContext.MultiDocumentParser(_session.Context, result))
            {
                while (parser._parser.BufferOffset + 2 != parser._parser.BufferSize)
                {
                    yield return parser.Parse(BlittableJsonDocumentBuilder.UsageMode.None, "");
                }
            }
            result.Dispose();
        }

    }
}