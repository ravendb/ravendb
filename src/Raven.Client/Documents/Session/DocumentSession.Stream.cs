using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentSession
    {
        public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query)
        {
            var queryProvider = (IRavenQueryProvider)query.Provider;
            var docQuery = queryProvider.ToDocumentQuery<T>(query.Expression);
            return Stream(docQuery);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query, out StreamQueryStatistics streamQueryStats)
        {
            var queryProvider = (IRavenQueryProvider)query.Provider;
            var docQuery = queryProvider.ToDocumentQuery<T>(query.Expression);
            return Stream(docQuery, out streamQueryStats);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query)
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(query.IndexName, query.GetIndexQuery());

            RequestExecutor.Execute(command, Context, sessionId: _clientSessionId);
            using (var result = streamOperation.SetResult(command.Result))
            {
                return YieldResults(query, result);
            }
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats)
        {
            var stats = new StreamQueryStatistics();
            var streamOperation = new StreamOperation(this, stats);
            var command = streamOperation.CreateRequest(query.IndexName, query.GetIndexQuery());

            RequestExecutor.Execute(command, Context, sessionId: _clientSessionId);
            using (var result = streamOperation.SetResult(command.Result))
            {
                streamQueryStats = stats;

                return YieldResults(query, result);
            }
        }

        private IEnumerator<StreamResult<T>> YieldResults<T>(IDocumentQuery<T> query, IEnumerator<BlittableJsonReaderObject> enumerator)
        {
            var projections = ((DocumentQuery<T>)query).FieldsToFetchToken?.Projections;

            while (enumerator.MoveNext())
            {
                var json = enumerator.Current;
                query.InvokeAfterStreamExecuted(json);

                yield return CreateStreamResult<T>(json, projections);
            }
        }

        public void StreamInto<T>(IDocumentQuery<T> query, Stream output)
        {
            var streamOperation = new StreamOperation(this);
            var command = streamOperation.CreateRequest(query.IndexName, query.GetIndexQuery());

            RequestExecutor.Execute(command, Context, sessionId: _clientSessionId);

            using (command.Result.Response)
            using (command.Result.Stream)
            {
                command.Result.Stream.CopyTo(output);
            }
        }

        private StreamResult<T> CreateStreamResult<T>(BlittableJsonReaderObject json, string[] projectionFields)
        {
            var metadata = json.GetMetadata();
            var changeVector = BlittableJsonExtensions.GetChangeVector(metadata);
            var id = metadata.GetId();

            //TODO - Investigate why ConvertToEntity fails if we don't call ReadObject before
            json = Context.ReadObject(json, id);
            var entity = QueryOperation.Deserialize<T>(id, json, metadata, projectionFields, true, this);

            var streamResult = new StreamResult<T>
            {
                ChangeVector = changeVector,
                Id = id,
                Document = entity,
                Metadata = new MetadataAsDictionary(metadata)
            };
            return streamResult;
        }

        public IEnumerator<StreamResult<T>> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = int.MaxValue,
             string startAfter = null)
        {
            var streamOperation = new StreamOperation(this);

            var command = streamOperation.CreateRequest( startsWith, matches, start, pageSize, null, startAfter);
            RequestExecutor.Execute(command, Context, sessionId: _clientSessionId);
            using (var result = streamOperation.SetResult(command.Result))
            {
                while (result.MoveNext())
                {
                    var json = result.Current;

                    yield return CreateStreamResult<T>(json, null);
                }
            }
        }
    }

}
